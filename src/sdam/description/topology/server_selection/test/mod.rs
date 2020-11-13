use std::{collections::HashMap, time::Duration};

use bson::{doc, DateTime as BsonDateTime};
use chrono::{DateTime, NaiveDateTime, Utc};
use semver::VersionReq;
use serde::Deserialize;
use tokio::sync::RwLockWriteGuard;

use crate::{
    is_master::{IsMasterCommandResponse, IsMasterReply, LastWrite},
    options::StreamAddress,
    runtime::AsyncJoinHandle,
    sdam::{
        description::topology::{test::f64_ms_as_duration, TopologyType},
        ServerDescription,
        ServerType,
        TopologyDescription,
    },
    selection_criteria::TagSet,
    test::{
        EventClient,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
        CLIENT_OPTIONS,
        LOCK,
    },
    RUNTIME,
};

mod in_window;
mod logic;

#[derive(Debug, Deserialize)]
struct TestTopologyDescription {
    #[serde(rename = "type")]
    topology_type: TopologyType,
    servers: Vec<TestServerDescription>,
}

impl TestTopologyDescription {
    fn into_topology_description(
        self,
        heartbeat_frequency: Option<Duration>,
    ) -> Option<TopologyDescription> {
        let servers: Option<Vec<ServerDescription>> = self
            .servers
            .into_iter()
        // The driver doesn't support server versions low enough not to support max staleness, so we
        // just manually filter them out here.
            .filter(|server| server.max_wire_version.map(|version| version >= 5).unwrap_or(true))
            .map(|sd| sd.into_server_description())
            .collect();

        let servers = match servers {
            Some(servers) => servers,
            None => return None,
        };

        TopologyDescription {
            single_seed: servers.len() == 1,
            topology_type: self.topology_type,
            set_name: None,
            max_set_version: None,
            max_election_id: None,
            compatibility_error: None,
            session_support_status: Default::default(),
            cluster_time: None,
            local_threshold: None,
            heartbeat_freq: heartbeat_frequency,
            servers: servers
                .into_iter()
                .map(|server| (server.address.clone(), server))
                .collect(),
        }
        .into()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestServerDescription {
    address: String,
    #[serde(rename = "avg_rtt_ms")]
    avg_rtt_ms: Option<f64>,
    #[serde(rename = "type")]
    server_type: TestServerType,
    tags: Option<TagSet>,
    last_update_time: Option<i32>,
    last_write: Option<LastWriteDate>,
    max_wire_version: Option<i32>,
}

impl TestServerDescription {
    fn into_server_description(self) -> Option<ServerDescription> {
        let server_type = match self.server_type.into_server_type() {
            Some(server_type) => server_type,
            None => return None,
        };

        let mut command_response = is_master_response_from_server_type(server_type);
        command_response.tags = self.tags;
        command_response.last_write = self.last_write.map(|last_write| LastWrite {
            last_write_date: utc_datetime_from_millis(last_write.last_write_date),
        });

        let is_master = IsMasterReply {
            command_response,
            round_trip_time: self.avg_rtt_ms.map(f64_ms_as_duration),
            cluster_time: None,
        };

        let mut server_desc = ServerDescription::new(
            StreamAddress::parse(&self.address).unwrap(),
            Some(Ok(is_master)),
        );
        server_desc.last_update_time = self
            .last_update_time
            .map(|i| utc_datetime_from_millis(i as i64));

        Some(server_desc)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LastWriteDate {
    last_write_date: i64,
}

#[derive(Clone, Copy, Debug, Deserialize)]
enum TestServerType {
    Standalone,
    Mongos,
    RSPrimary,
    RSSecondary,
    RSArbiter,
    RSOther,
    RSGhost,
    Unknown,
    PossiblePrimary,
}

impl TestServerType {
    fn into_server_type(self) -> Option<ServerType> {
        match self {
            TestServerType::Standalone => Some(ServerType::Standalone),
            TestServerType::Mongos => Some(ServerType::Mongos),
            TestServerType::RSPrimary => Some(ServerType::RSPrimary),
            TestServerType::RSSecondary => Some(ServerType::RSSecondary),
            TestServerType::RSArbiter => Some(ServerType::RSArbiter),
            TestServerType::RSOther => Some(ServerType::RSOther),
            TestServerType::RSGhost => Some(ServerType::RSGhost),
            TestServerType::Unknown => Some(ServerType::Unknown),
            TestServerType::PossiblePrimary => None,
        }
    }
}

fn utc_datetime_from_millis(millis: i64) -> BsonDateTime {
    let seconds_portion = millis / 1000;
    let nanos_portion = (millis % 1000) * 1_000_000;

    let naive_datetime = NaiveDateTime::from_timestamp(seconds_portion, nanos_portion as u32);
    let datetime = DateTime::from_utc(naive_datetime, Utc);

    BsonDateTime(datetime)
}

fn is_master_response_from_server_type(server_type: ServerType) -> IsMasterCommandResponse {
    let mut response = IsMasterCommandResponse::default();

    match server_type {
        ServerType::Unknown => {
            response.ok = Some(0.0);
        }
        ServerType::Mongos => {
            response.ok = Some(1.0);
            response.msg = Some("isdbgrid".into());
        }
        ServerType::RSPrimary => {
            response.ok = Some(1.0);
            response.set_name = Some("foo".into());
            response.is_master = Some(true);
        }
        ServerType::RSOther => {
            response.ok = Some(1.0);
            response.set_name = Some("foo".into());
            response.hidden = Some(true);
        }
        ServerType::RSSecondary => {
            response.ok = Some(1.0);
            response.set_name = Some("foo".into());
            response.secondary = Some(true);
        }
        ServerType::RSArbiter => {
            response.ok = Some(1.0);
            response.set_name = Some("foo".into());
            response.arbiter_only = Some(true);
        }
        ServerType::RSGhost => {
            response.ok = Some(1.0);
            response.is_replica_set = Some(true);
        }
        ServerType::Standalone => {
            response.ok = Some(1.0);
        }
    };

    response
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(threaded_scheduler))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn load_balancing_test() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut setup_client_options = CLIENT_OPTIONS.clone();
    setup_client_options.hosts.drain(1..);
    setup_client_options.direct_connection = Some(true);
    let setup_client = TestClient::with_options(Some(setup_client_options)).await;

    let version = VersionReq::parse(">= 4.2.9").unwrap();
    // blockConnection failpoint option only supported in 4.2.9+.
    if !version.matches(&setup_client.server_version) {
        println!(
            "skipping load_balancing_test test due to server not supporting blockConnection option"
        );
        return;
    }

    if !setup_client.is_sharded() {
        println!("skipping load_balancing_test test due to topology not being sharded");
        return;
    }

    if CLIENT_OPTIONS.hosts.len() != 2 {
        println!("skipping load_balancing_test test due to topology not having 2 mongoses");
        return;
    }

    let options = FailCommandOptions::builder()
        .block_connection(Duration::from_millis(500))
        .build();
    let failpoint = FailPoint::fail_command(&["find"], FailPointMode::AlwaysOn, options);

    let fp_guard = setup_client
        .enable_failpoint(failpoint, None)
        .await
        .expect("enabling failpoint should succeed");

    let mut client = EventClient::new().await;

    /// min_share is the lower bound for the % of times the the less selected server
    /// was selected. max_share is the upper bound.
    async fn do_test(client: &mut EventClient, min_share: f64, max_share: f64) {
        client.command_events.write().unwrap().clear();

        let mut handles: Vec<AsyncJoinHandle<()>> = Vec::new();
        for _ in 0..10 {
            let collection = client
                .database("load_balancing_test")
                .collection("load_balancing_test");
            handles.push(
                RUNTIME
                    .spawn(async move {
                        for _ in 0..10 {
                            let _ = collection.find_one(None, None).await;
                        }
                    })
                    .unwrap(),
            )
        }

        futures::future::join_all(handles).await;

        let mut tallies: HashMap<StreamAddress, u32> = HashMap::new();
        for event in client.get_command_started_events("find") {
            *tallies.entry(event.connection.address.clone()).or_insert(0) += 1;
        }

        assert_eq!(tallies.len(), 2);
        let mut counts: Vec<_> = tallies.values().collect();
        counts.sort();

        // verify that the lesser picked server (slower one) was picked less than 25% of the time.
        let share_of_selections = (*counts[0] as f64) / ((*counts[0] + *counts[1]) as f64);
        println!("selections: {}", share_of_selections);
        assert!(share_of_selections <= max_share);
        assert!(share_of_selections >= min_share);
    }

    do_test(&mut client, 0.05, 0.25).await;

    // disable failpoint and rerun, should be close to even split
    drop(fp_guard);
    do_test(&mut client, 0.40, 0.50).await;
}
