/*
 * Copyright © 2024 Baird Creek Software LLC
 *
 * Licensed under the PolyForm Noncommercial License, version 1.0.0;
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     https://polyformproject.org/licenses/noncommercial/1.0.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package is.galia.plugin.azurestorage.config;

public enum Key {

    AZUREBLOBSTORAGECACHE_ACCOUNT_KEY     ("cache.AzureBlobStorageCache.account_key"),
    AZUREBLOBSTORAGECACHE_ACCOUNT_NAME    ("cache.AzureBlobStorageCache.account_name"),
    AZUREBLOBSTORAGECACHE_CONTAINER_NAME  ("cache.AzureBlobStorageCache.container_name"),
    AZUREBLOBSTORAGECACHE_ENDPOINT        ("cache.AzureBlobStorageCache.endpoint"),
    AZUREBLOBSTORAGECACHE_BLOB_NAME_PREFIX("cache.AzureBlobStorageCache.blob_name_prefix"),

    AZUREBLOBSTORAGESOURCE_ACCOUNT_KEY     ("source.AzureBlobStorageSource.account_key"),
    AZUREBLOBSTORAGESOURCE_ACCOUNT_NAME    ("source.AzureBlobStorageSource.account_name"),
    AZUREBLOBSTORAGESOURCE_CHUNKING_ENABLED("source.AzureBlobStorageSource.chunking.enabled"),
    AZUREBLOBSTORAGESOURCE_CHUNK_SIZE      ("source.AzureBlobStorageSource.chunking.chunk_size"),
    AZUREBLOBSTORAGESOURCE_CONTAINER_NAME  ("source.AzureBlobStorageSource.container_name"),
    AZUREBLOBSTORAGESOURCE_ENDPOINT        ("source.AzureBlobStorageSource.endpoint"),
    AZUREBLOBSTORAGESOURCE_LOOKUP_STRATEGY ("source.AzureBlobStorageSource.lookup_strategy");

    private final String key;

    Key(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }

    @Override
    public String toString() {
        return key();
    }

}
