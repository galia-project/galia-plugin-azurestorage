# Changes since Cantaloupe 5.0.5

## Generally

* Updated the client SDK version.
* Non-Microsoft Azure endpoints are supported.
* Extensive code refactoring.

## AzureBlobStorageSource

* AzureStorageSource has been renamed to AzureBlobStorageSource.
* Multiple endpoints are supported when using DelegateLookupStrategy.
* The `azurestorage_blob_key()` delegate method has been redesigned as
  `azureblobstorage_blob_info()` and can return a hash with container, blob,
  and endpoint keys, enabling multiple endpoints and containers to be used
  simultaneously by the same application instance.

## AzureBlobStorageCache

* AzureStorageCache has been renamed to AzureBlobStorageCache.
* AzureBlobStorageCache supports last-accessed times if they are enabled at the 
  service level.
