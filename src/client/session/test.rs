use crate::{test::{CLIENT_OPTIONS, TestClient, EventClient, CommandEvent}, Client, RUNTIME, error::Result};
use std::{future::Future, time::Duration};
use bson::{Bson, doc};

macro_rules! db_op {
    ($db:ident, $body:expr) => {
        |client| async move {
            let $db = client.database("sessionopdb");
            $body.await.unwrap();
        }
    }
}

macro_rules! collection_op {
    ($coll:ident, $body:expr) => {
        |client| async move {
            let $coll = client.database("sessionopdb").collection("sessionopcoll");
            $body.await.unwrap();
        }
    }
}

macro_rules! for_each_op {
	($test_func:ident) => {{
        // collection read operations
        $test_func(
            "aggregate",
            collection_op!(coll, coll.aggregate(vec![doc! { "$match": { "x": 1 } }], None))
        ).await;
        $test_func("find", collection_op!(coll, coll.find(doc! { "x": 1 }, None))).await;
        $test_func("find", collection_op!(coll, coll.find_one(doc! { "x": 1 }, None))).await;
        $test_func("distinct", collection_op!(coll, coll.distinct("x", None, None))).await;
        $test_func("aggregate", collection_op!(coll, coll.count_documents(None, None))).await;

        // collection write operations
        $test_func("insert", collection_op!(coll, coll.insert_one(doc! { "x": 1 }, None))).await;
        $test_func("insert", collection_op!(coll, coll.insert_many(vec![doc! { "x": 1 }], None))).await;
        $test_func("update", collection_op!(coll, coll.replace_one(doc! { "x": 1 }, doc! { "x": 2 }, None))).await;
        $test_func("update", collection_op!(coll, coll.update_one(doc! {}, doc! { "$inc": {"x": 5 } }, None))).await;
        $test_func("update", collection_op!(coll, coll.update_many(doc! {}, doc! { "$inc": {"x": 5 } }, None))).await;
        $test_func("delete", collection_op!(coll, coll.delete_one(doc! { "x": 1 }, None))).await;
        $test_func("delete", collection_op!(coll, coll.delete_many(doc! { "x": 1 }, None))).await;
        $test_func("findAndModify", collection_op!(coll, coll.find_one_and_delete(doc! { "x": 1 }, None))).await;
        $test_func(
            "findAndModify",
            collection_op!(coll, coll.find_one_and_update(doc! {}, doc! { "$inc": { "x": 1 } }, None))
        ).await;
        $test_func("findAndModify", collection_op!(coll, coll.find_one_and_replace(doc! {}, doc! {"x": 1}, None))).await;
        $test_func("drop", collection_op!(coll, coll.drop(None))).await;

        // db operations
        $test_func("listCollections", db_op!(db, db.list_collections(None, None))).await;
        // $test_func("ping", db_op!(db, db.run_command(doc! { "ping":  1 }, None))).await;
        $test_func("create", db_op!(db, db.create_collection("sessionopcoll", None))).await;
        $test_func("dropDatabase", db_op!(db, db.drop(None))).await;
    }};
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn pool_is_lifo() {
    let client = TestClient::new().await;

    if client.is_standalone() {
        return;
    }
    
    let timeout = Duration::from_secs(60 * 60);
    
    let a = client.start_session_with_timeout(timeout).await;
    let b = client.start_session_with_timeout(timeout).await;

    let a_id = a.id().clone();
    let b_id = b.id().clone();

    // End both sessions, waiting after each to ensure the background task got scheduled
    // in the Drop impls.
    drop(a);
    RUNTIME.delay_for(Duration::from_millis(250)).await;

    drop(b);
    RUNTIME.delay_for(Duration::from_millis(250)).await;
    
    let s1 = client.start_session_with_timeout(timeout).await;
    assert_eq!(s1.id(), &b_id);

    let s2 = client.start_session_with_timeout(timeout).await;
    assert_eq!(s2.id(), &a_id);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn cluster_time_in_commands() {
    let client = TestClient::new().await;
    if client.is_standalone() {
        return;
    }

    async fn cluster_time_test<F, G, R>(command_name: &str, operation: F)
    where
        F: Fn(Client) -> G,
        G: Future<Output = Result<R>>,
    {
        let mut options = CLIENT_OPTIONS.clone();
        options.heartbeat_freq = Some(Duration::from_secs(1000));
        let client = EventClient::with_options(options).await;
        println!("new client");
        
        operation(client.clone()).await.expect("operation should succeed");
        let (command_started, command_succeeded) = client.get_successful_command_execution(command_name);
        
        assert!(command_started.command.get("$clusterTime").is_some());

        let response_cluster_time = command_succeeded
            .reply
            .get("$clusterTime")
            .expect("should get cluster time from command response");
        
        operation(client.clone()).await.expect("operation should succeed");
        let (command_started, command_succeded) = client.get_successful_command_execution(command_name);
        
        assert_eq!(
            response_cluster_time,
            command_started.command.get("$clusterTime").expect("second command should contain cluster time"),
            "cluster time not equal for {}",
            command_name
        );
    }
    
    // cluster_time_test("ping", |client| async move {
    //     client.database(function_name!()).run_command(doc! { "ping": 1 }, None).await
    // }).await;

    // cluster_time_test("aggregate", |client| async move {
    //     client.database(function_name!()).collection(function_name!())
    //         .aggregate(vec![doc! { "$match": { "x": 1 } }], None).await
    // }).await;

    // cluster_time_test("find", |client| async move {
    //     client.database(function_name!()).collection(function_name!()).find(doc! {}, None).await
    // }).await;

    cluster_time_test("insert", |client| async move {
        println!("starting insert");
        client.database(function_name!()).collection(function_name!()).insert_one(doc! {}, None).await
    }).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn session_usage() {
    let client = TestClient::new().await;
    if client.is_standalone() {
        return;
    }

    async fn session_usage_test<F, G>(command_name: &str, operation: F)
    where
        F: Fn(Client) -> G,
        G: Future<Output = ()>,
    {
        let client = EventClient::new().await;
        operation(client.clone()).await;
        let (command_started, _) = client.get_successful_command_execution(command_name);
        assert!(command_started.command.get("lsid").is_some(), "implicit session passed to {}", command_name);
    }

    for_each_op!(session_usage_test)
}
