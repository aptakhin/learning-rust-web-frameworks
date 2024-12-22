use tokio::runtime::Handle;

use axum::{
    async_trait,
    extract::{FromRef, FromRequestParts, State},
    http::{request::Parts, StatusCode},
    routing::{get},
    Router,
};

use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use std::sync::mpsc;
use std::time::Duration;

fn app() -> Router<PgPool> {
    let db_connection_str = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/manyevents".to_string());

    let mut router = Router::new()
        .route("/", get(|| async { "Hello, World!" }));

    let (tx, rx) = mpsc::channel();

    // TODO: Issue to run async code sync function to bootstrap database
    let handle = Handle::current();
    std::thread::spawn(move || {
        handle.block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(5)
                .acquire_timeout(Duration::from_secs(3))
                .connect(&db_connection_str)
                .await
                .expect("can't connect to database");

            router = router.route(
                    "/db",
                    get(using_connection_pool_extractor).post(using_connection_extractor),
                )
                .with_state(pool);

            tx.send(router).expect("Failed to send router");
        });
    });

    router = rx.recv().expect("Failed to receive router");

    router
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("{}=debug", env!("CARGO_CRATE_NAME")).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    // TODO: `Serve<Router<Pool<Postgres>>, _>` is not a future
    axum::serve(listener, app()).await.unwrap();
}

async fn using_connection_pool_extractor(
    State(pool): State<PgPool>,
) -> Result<String, (StatusCode, String)> {
    sqlx::query_scalar("select 'hello world from pg'")
        .fetch_one(&pool)
        .await
        .map_err(internal_error)
}

struct DatabaseConnection(sqlx::pool::PoolConnection<sqlx::Postgres>);

#[async_trait]
impl<S> FromRequestParts<S> for DatabaseConnection
where
    PgPool: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(_parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let pool = PgPool::from_ref(state);
        let conn = pool.acquire().await.map_err(internal_error)?;
        Ok(Self(conn))
    }
}

async fn using_connection_extractor(
    DatabaseConnection(mut conn): DatabaseConnection,
) -> Result<String, (StatusCode, String)> {
    sqlx::query_scalar("select 'hello world from pg'")
        .fetch_one(&mut *conn)
        .await
        .map_err(internal_error)
}

fn internal_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
{
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}


#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use rstest::{rstest};

    #[rstest]
    #[tokio::test]
    async fn get_root() {
        let app = app();

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"Hello, World!");
    }

    #[rstest]
    #[tokio::test]
    async fn get_db() {
        let app = app();

        // TODO: Issue to
        // error[E0599]: the method `oneshot` exists for struct `Router<Pool<Postgres>>`, but its trait bounds were not satisfied
        let response = app
            .oneshot(Request::builder().uri("/db").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"Hello, World!");
    }
}
