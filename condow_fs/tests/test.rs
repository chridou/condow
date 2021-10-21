use condow_fs::{Condow, FsClient};

fn create_condow_condow() -> Condow<FsClient> {
    FsClient::condow(Default::default()).unwrap()
}

fn get_test_file_path() -> String {
    format!(
        "{}/tests/test_data",
        std::env::current_dir().unwrap().display()
    )
}

#[tokio::test]
async fn download_full() {
    let condow = create_condow_condow();

    let data = condow
        .download(get_test_file_path(), ..)
        .await
        .unwrap()
        .into_vec()
        .await
        .unwrap();

    assert_eq!(&data[..], b"abcdefghijklmnopqrstuvwxyz");
}
