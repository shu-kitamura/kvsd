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

    >
    ```

## TODO

* ポート、データディレクトリなどをオプションで指定できるようにすること。
