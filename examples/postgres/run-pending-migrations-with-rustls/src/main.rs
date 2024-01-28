use std::sync::Arc;

use diesel::{ConnectionError, ConnectionResult};
use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_async::AsyncPgConnection;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use futures_util::future::{BoxFuture, Either};
use futures_util::FutureExt;
use tokio::sync::{broadcast, oneshot};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Should be in the form of postgres://user:password@localhost/database?sslmode=require
    let db_url = std::env::var("DATABASE_URL").expect("Env var `DATABASE_URL` not set");

    let async_connection = establish_connection(db_url.as_str()).await?;

    let mut async_wrapper: AsyncConnectionWrapper<AsyncPgConnection> =
        AsyncConnectionWrapper::from(async_connection);

    tokio::task::spawn_blocking(move || {
        async_wrapper.run_pending_migrations(MIGRATIONS).unwrap();
    })
    .await?;

    Ok(())
}

fn establish_connection(config: &str) -> BoxFuture<ConnectionResult<AsyncPgConnection>> {
    let fut = async {
        // We first set up the way we want rustls to work.
        let rustls_config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_certs())
            .with_no_client_auth();
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(rustls_config);
        let (client, conn) = tokio_postgres::connect(config, tls)
            .await
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        let (tx, rx) = broadcast::channel(1);
        let (conn_tx, conn_rx) = oneshot::channel();

        tokio::spawn(async move {
            match futures_util::future::select(conn_rx, conn).await {
                Either::Left(_) | Either::Right((Ok(_), _)) => {}
                Either::Right((Err(e), _)) => {
                    let _ = tx.send(Arc::new(e));
                }
            }
        });

        AsyncPgConnection::try_from(client, Some(rx), Some(conn_tx)).await
    };
    fut.boxed()
}

fn root_certs() -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    let certs = rustls_native_certs::load_native_certs().expect("Certs not loadable!");
    let certs: Vec<_> = certs.into_iter().map(|cert| cert.0).collect();
    roots.add_parsable_certificates(&certs);
    roots
}
