use axum::{
    async_trait,
    extract::{FromRef, FromRequestParts, State},
    http::{request::Parts, StatusCode},
    routing::get,
    Router,
};

use http_body_util::BodyExt;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;
use tokio::net::TcpListener;
use tower::util::ServiceExt;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

async fn make_db() -> PgPool {
    let db_connection_str = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/manyevents".to_string());

    PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(&db_connection_str)
        .await
        .expect("can't connect to database")
}

async fn routes_app() -> Router<()> {
    let pool = make_db().await;

    let router: Router<()> = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route(
            "/db",
            get(using_connection_pool_extractor).post(using_connection_extractor),
        )
        .with_state(pool);

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

    let app = routes_app().await;
    axum::serve(listener, app).await.unwrap();
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
    use rstest::{fixture, rstest};

    #[fixture]
    async fn app() -> Router<()> {
        routes_app().await
    }

    #[fixture]
    async fn conn() -> sqlx::pool::PoolConnection<sqlx::Postgres> {
        let pool = make_db().await;
        pool.acquire().await.expect("Error connection")
    }

    #[rstest]
    #[tokio::test]
    async fn get_root(#[future] app: Router<()>) {
        let response = app
            .await
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"Hello, World!");
    }

    #[rstest]
    #[tokio::test]
    async fn get_db(
        #[future] app: Router<()>,
        #[future] conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
    ) {
        let response = app
            .await
            .oneshot(Request::builder().uri("/db").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let f: Result<String, (StatusCode, String)> =
            sqlx::query_scalar("select 'hello world from pg'")
                .fetch_one(&mut *(conn.await))
                .await
                .map_err(internal_error);

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert_eq!(body_str, "hello world from pg");
        assert_eq!(f.unwrap(), "hello world from pg");
    }
}
