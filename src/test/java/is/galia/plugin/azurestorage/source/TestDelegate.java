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

package is.galia.plugin.azurestorage.source;

import is.galia.config.Configuration;
import is.galia.delegate.Delegate;
import is.galia.plugin.Plugin;
import is.galia.plugin.azurestorage.config.Key;
import is.galia.resource.RequestContext;

import java.util.Map;
import java.util.Set;

public class TestDelegate implements Delegate, Plugin {

    private RequestContext requestContext;

    //region Plugin methods

    @Override
    public Set<String> getPluginConfigKeys() {
        return Set.of();
    }

    @Override
    public String getPluginName() {
        return TestDelegate.class.getSimpleName();
    }

    @Override
    public void onApplicationStart() {}

    @Override
    public void initializePlugin() {}

    @Override
    public void onApplicationStop() {}

    //endregion
    //region Delegate methods

    @Override
    public void setRequestContext(RequestContext context) {
        this.requestContext = context;
    }

    @Override
    public RequestContext getRequestContext() {
        return requestContext;
    }

    @SuppressWarnings("unused")
    public Object azureblobstoragesource_blob_info() {
        Configuration config = Configuration.forApplication();
        String containerName = config.getString(Key.AZUREBLOBSTORAGESOURCE_CONTAINER_NAME.key());
        String identifier    = getRequestContext().getIdentifier().toString();
        return switch (identifier) {
            case "missing"    -> null;
            case "ghost1.png" -> Map.of(
                    "container", containerName,
                    "name", "ghost1");
            default           -> Map.of(
                    "container", containerName,
                    "name", identifier);
        };
    }

}
