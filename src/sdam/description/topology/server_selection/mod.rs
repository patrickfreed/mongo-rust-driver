#[cfg(test)]
mod test;

use std::{collections::HashMap, fmt, sync::Arc, time::Duration};

use approx::abs_diff_ne;
use rand::{
    rngs::SmallRng,
    seq::{IteratorRandom, SliceRandom},
    SeedableRng,
};

use super::TopologyDescription;
use crate::{
    error::{ErrorKind, Result},
    options::StreamAddress,
    sdam::{
        description::{
            server::{ServerDescription, ServerType},
            topology::TopologyType,
        },
        public::ServerInfo,
        Server,
    },
    selection_criteria::{ReadPreference, SelectionCriteria, TagSet},
};

const DEFAULT_LOCAL_THRESHOLD: Duration = Duration::from_millis(15);

/// Attempt to select a server, returning None if no server could be selected
/// that matched the provided criteria.
pub(crate) fn attempt_to_select_server<'a>(
    criteria: &'a SelectionCriteria,
    topology_description: &'a TopologyDescription,
    servers: &'a HashMap<StreamAddress, Arc<Server>>,
) -> Result<Option<Arc<Server>>> {
    let in_window = topology_description.suitable_servers_in_latency_window(criteria)?;
    let in_window_servers = in_window
        .into_iter()
        .flat_map(|desc| servers.get(&desc.address))
        .collect();
    Ok(select_server_in_latency_window(in_window_servers))
}

/// Choose a server from several suitable choices within the latency window according to
/// the algorithm laid out in the server selection specification.
///
/// This is the last stage of server selection and should not be called directly.
pub(crate) fn select_server_in_latency_window(in_window: Vec<&Arc<Server>>) -> Option<Arc<Server>> {
    if in_window.is_empty() {
        return None;
    } else if in_window.len() == 1 {
        return Some(in_window[0].clone());
    }

    let mut rng = SmallRng::from_entropy();
    let mut choices = in_window.into_iter().choose_multiple(&mut rng, 2);
    choices.shuffle(&mut rng);

    let server1 = choices[0];
    let server2 = choices[1];

    fn op_count(server: &Server) -> u32 {
        server.active_connection_count() + server.wait_queue_length()
    }

    // choose server with min active connection count only if the counts differ by more than
    // 5% of max_pool_size
    if abs_diff_ne!(
        op_count(server1.as_ref()),
        op_count(server2.as_ref()),
        epsilon = std::cmp::max(1, server1.max_pool_size() / 20)
    ) {
        choices
            .into_iter()
            .min_by_key(|s| op_count(s.as_ref()))
            .cloned()
    } else {
        // otherwise choose the one with most available connections
        choices
            .into_iter()
            .max_by_key(|s| s.available_connection_count())
            .or(Some(server1))
            .cloned()
    }
}

impl TopologyDescription {
    pub(crate) fn server_selection_timeout_error_message(
        &self,
        criteria: &SelectionCriteria,
    ) -> String {
        if self.has_available_servers() {
            format!(
                "Server selection timeout: None of the available servers suitable for criteria \
                 {:?}. Topology: {}",
                criteria, self
            )
        } else {
            format!(
                "Server selection timeout: No available servers. Topology: {}",
                self
            )
        }
    }

    pub(crate) fn suitable_servers_in_latency_window<'a>(
        &'a self,
        criteria: &'a SelectionCriteria,
    ) -> Result<Vec<&'a ServerDescription>> {
        if let Some(message) = self.compatibility_error() {
            return Err(ErrorKind::ServerSelectionError {
                message: message.to_string(),
            }
            .into());
        }

        let mut suitable_servers = match criteria {
            SelectionCriteria::ReadPreference(ref read_pref) => self.suitable_servers(read_pref)?,
            SelectionCriteria::Predicate(ref filter) => self
                .servers
                .values()
                .filter(|s| filter(&ServerInfo::new(s)))
                .collect(),
        };

        // If the read preference is primary, we skip the overhead of calculating the latency window
        // because we know that there's only one server selected.
        if !criteria.is_read_pref_primary() {
            self.retain_servers_within_latency_window(&mut suitable_servers);
        }

        // Choose a random server from the remaining set.
        Ok(suitable_servers)
    }

    fn has_available_servers(&self) -> bool {
        self.servers.values().any(|server| server.is_available())
    }

    fn suitable_servers<'a>(
        &'a self,
        read_preference: &'a ReadPreference,
    ) -> Result<Vec<&'a ServerDescription>> {
        let servers = match self.topology_type {
            TopologyType::Unknown => Vec::new(),
            TopologyType::Single => self.servers.values().collect(),
            TopologyType::Sharded => self.servers_with_type(&[ServerType::Mongos]).collect(),
            TopologyType::ReplicaSetWithPrimary | TopologyType::ReplicaSetNoPrimary => {
                self.suitable_servers_in_replica_set(read_preference)?
            }
        };

        Ok(servers)
    }

    fn retain_servers_within_latency_window<'a>(
        &self,
        suitable_servers: &mut Vec<&'a ServerDescription>,
    ) {
        let shortest_average_rtt = suitable_servers
            .iter()
            .filter_map(|server_desc| server_desc.average_round_trip_time)
            .fold(Option::<Duration>::None, |min, curr| match min {
                Some(prev) => Some(prev.min(curr)),
                None => Some(curr),
            });

        let local_threshold = self.local_threshold.unwrap_or(DEFAULT_LOCAL_THRESHOLD);

        let max_rtt_within_window = shortest_average_rtt.map(|rtt| rtt + local_threshold);

        suitable_servers.retain(move |server_desc| {
            if let Some(server_rtt) = server_desc.average_round_trip_time {
                if let Some(max_rtt) = max_rtt_within_window {
                    return server_rtt <= max_rtt;
                }
            }

            false
        });
    }

    fn servers_with_type<'a>(
        &'a self,
        types: &'a [ServerType],
    ) -> impl Iterator<Item = &'a ServerDescription> {
        self.servers
            .values()
            .filter(move |server| types.contains(&server.server_type))
    }

    fn suitable_servers_in_replica_set<'a>(
        &'a self,
        read_preference: &'a ReadPreference,
    ) -> Result<Vec<&'a ServerDescription>> {
        let servers = match read_preference {
            ReadPreference::Primary => self.servers_with_type(&[ServerType::RSPrimary]).collect(),
            ReadPreference::Secondary { ref options } => self
                .suitable_servers_for_read_preference(
                    &[ServerType::RSSecondary],
                    options.tag_sets.as_ref(),
                    options.max_staleness,
                )?,
            ReadPreference::PrimaryPreferred { ref options } => {
                match self.servers_with_type(&[ServerType::RSPrimary]).next() {
                    Some(primary) => vec![primary],
                    None => self.suitable_servers_for_read_preference(
                        &[ServerType::RSSecondary],
                        options.tag_sets.as_ref(),
                        options.max_staleness,
                    )?,
                }
            }
            ReadPreference::SecondaryPreferred { ref options } => {
                let suitable_servers = self.suitable_servers_for_read_preference(
                    &[ServerType::RSSecondary],
                    options.tag_sets.as_ref(),
                    options.max_staleness,
                )?;

                if suitable_servers.is_empty() {
                    self.servers_with_type(&[ServerType::RSPrimary]).collect()
                } else {
                    suitable_servers
                }
            }
            ReadPreference::Nearest { ref options } => self.suitable_servers_for_read_preference(
                &[ServerType::RSPrimary, ServerType::RSSecondary],
                options.tag_sets.as_ref(),
                options.max_staleness,
            )?,
        };

        Ok(servers)
    }

    fn suitable_servers_for_read_preference<'a>(
        &'a self,
        types: &'a [ServerType],
        tag_sets: Option<&'a Vec<TagSet>>,
        max_staleness: Option<Duration>,
    ) -> Result<Vec<&'a ServerDescription>> {
        super::verify_max_staleness(max_staleness)?;

        let mut servers = self.servers_with_type(types).collect();

        // We don't need to check for the Client's default max_staleness because it would be passed
        // in as part of the Client's default ReadPreference if none is specified for the operation.
        if let Some(max_staleness) = max_staleness {
            // According to the spec, max staleness <= 0 is the same as no max staleness.
            if max_staleness > Duration::from_secs(0) {
                self.filter_servers_by_max_staleness(&mut servers, max_staleness);
            }
        }

        if let Some(tag_sets) = tag_sets {
            filter_servers_by_tag_sets(&mut servers, tag_sets);
        }

        Ok(servers)
    }

    fn filter_servers_by_max_staleness(
        &self,
        servers: &mut Vec<&ServerDescription>,
        max_staleness: Duration,
    ) {
        let primary = self
            .servers
            .values()
            .find(|server| server.server_type == ServerType::RSPrimary);

        match primary {
            Some(primary) => {
                self.filter_servers_by_max_staleness_with_primary(servers, primary, max_staleness)
            }
            None => self.filter_servers_by_max_staleness_without_primary(servers, max_staleness),
        };
    }

    fn filter_servers_by_max_staleness_with_primary(
        &self,
        servers: &mut Vec<&ServerDescription>,
        primary: &ServerDescription,
        max_staleness: Duration,
    ) {
        let max_staleness_ms = max_staleness.as_millis() as i64;

        servers.retain(|server| {
            let server_staleness = self.calculate_secondary_staleness_with_primary(server, primary);

            server_staleness
                .map(|staleness| staleness <= max_staleness_ms)
                .unwrap_or(false)
        })
    }

    fn filter_servers_by_max_staleness_without_primary(
        &self,
        servers: &mut Vec<&ServerDescription>,
        max_staleness: Duration,
    ) {
        let max_staleness = max_staleness.as_millis() as i64;
        let max_write_date = self
            .servers
            .values()
            .filter(|server| server.server_type == ServerType::RSSecondary)
            .filter_map(|server| {
                server
                    .last_write_date()
                    .ok()
                    .and_then(std::convert::identity)
            })
            .map(|last_write_date| last_write_date.timestamp_millis())
            .max();

        let secondary_max_write_date = match max_write_date {
            Some(max_write_date) => max_write_date,
            None => return,
        };

        servers.retain(|server| {
            let server_staleness = self
                .calculate_secondary_staleness_without_primary(server, secondary_max_write_date);

            server_staleness
                .map(|staleness| staleness <= max_staleness)
                .unwrap_or(false)
        })
    }

    fn calculate_secondary_staleness_with_primary(
        &self,
        secondary: &ServerDescription,
        primary: &ServerDescription,
    ) -> Option<i64> {
        let primary_last_update = primary.last_update_time?.timestamp_millis();
        let primary_last_write = primary.last_write_date().ok()??.timestamp_millis();

        let secondary_last_update = secondary.last_update_time?.timestamp_millis();
        let secondary_last_write = secondary.last_write_date().ok()??.timestamp_millis();

        let heartbeat_frequency = self.heartbeat_frequency().as_millis() as i64;

        let staleness = (secondary_last_update - secondary_last_write)
            - (primary_last_update - primary_last_write)
            + heartbeat_frequency;

        Some(staleness)
    }

    fn calculate_secondary_staleness_without_primary(
        &self,
        secondary: &ServerDescription,
        max_last_write_date: i64,
    ) -> Option<i64> {
        let secondary_last_write = secondary.last_write_date().ok()??.timestamp_millis();
        let heartbeat_frequency = self.heartbeat_frequency().as_millis() as i64;

        let staleness = max_last_write_date - secondary_last_write + heartbeat_frequency;
        Some(staleness)
    }
}

impl fmt::Display for TopologyDescription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{ Type: {:?}, Servers: [ ", self.topology_type)?;
        for server_info in self.servers.values().map(ServerInfo::new) {
            write!(f, "{}, ", server_info)?;
        }
        write!(f, "] }}")
    }
}

fn filter_servers_by_tag_sets(servers: &mut Vec<&ServerDescription>, tag_sets: &[TagSet]) {
    if tag_sets.is_empty() {
        return;
    }

    for tag_set in tag_sets {
        let matches_tag_set = |server: &&ServerDescription| server.matches_tag_set(tag_set);

        if servers.iter().any(matches_tag_set) {
            servers.retain(matches_tag_set);

            return;
        }
    }

    servers.clear();
}
