use compio::runtime::RuntimeBuilder;
use mrky::{db::DBManager, server::Server};

fn main() {
    RuntimeBuilder::new().build().unwrap().block_on(async {
        let db_manager = DBManager::new();

        let addr = "127.0.0.1:6381".parse().unwrap();
        let server = Server::new(addr);
        server.run(db_manager.clone()).await.unwrap();
    });
}
