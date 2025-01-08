use compio::runtime::RuntimeBuilder;
use mrky::{db::DBManager, server::Server};
use tracing::{info, info_span, span, Level};
use tracing_subscriber::FmtSubscriber;

fn main() {
    let log_subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(log_subscriber).unwrap();
    let span = info_span!("main");
    let _enter = span.enter();
    RuntimeBuilder::new().build().unwrap().block_on(async {
        

        let db_manager = DBManager::new();

        let addr = "127.0.0.1:6381".parse().unwrap();
        info!("listen addr {}", addr);
        let server = Server::new(addr);
        server.run(db_manager.clone()).await.unwrap();
    });
}
