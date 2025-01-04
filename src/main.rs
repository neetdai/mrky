use compio::runtime::RuntimeBuilder;

fn main() {
    RuntimeBuilder::new()
        .build()
        .unwrap()
        .block_on(async {

        });
}