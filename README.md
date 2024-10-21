# kvsd

kvsd は Rust で実装したキーバリューストアです。

## 使用方法

* サーバ側

    ```
    $ kvsd
    ```

* クライアント側

    ```
    $ kvsh
    > get k1
    value1
    > put test test_value
    > get test
    test_value
    > delete test
    > get test

    > exit
    ```

## TODO

* ポート、データディレクトリなどをコマンドライン引数で指定できるようにする
* サブコマンド init(仮) で、データディレクトリや wal などの必要なファイルを作成する
* クライアント側で不正なコマンドを受け取ったときに usage を表示する
