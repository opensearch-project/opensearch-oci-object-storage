package org.opensearch.fixtures.oci;

import com.google.common.base.Preconditions;
import com.oracle.bmc.model.BmcException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.commons.io.IOUtils;

@Value
public class LocalBucket {
    Map<String, OSObject> prefixToObjectMap = new ConcurrentHashMap<>();
    String name;

    public void putObject(String objectName, InputStream inputStream, int contentLength)
            throws IOException {
        Preconditions.checkArgument(
                contentLength < Integer.MAX_VALUE,
                "Local object storage is currently not designed to handle very large objects");
        final byte[] objectData = IOUtils.readFully(inputStream, contentLength);
        final OSObject osObject = new OSObject(objectName, objectData);
        prefixToObjectMap.put(objectName, osObject);
    }

    public OSObject getObject(String objectName) {
        return prefixToObjectMap.get(objectName);
    }

    public OSObject deleteObject(String objectName) {
        if (!prefixToObjectMap.containsKey(objectName)) {
            throw new BmcException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "object not found",
                    "object not found",
                    "object not found");
        }

        return prefixToObjectMap.remove(objectName);
    }

    public List<OSObject> getObjectByPrefix(String prefix) {
        return prefixToObjectMap.values().stream()
                .sorted(Comparator.comparing(OSObject::getPrefix))
                .filter(object -> object.getPrefix().contains(prefix))
                .collect(Collectors.toList());
    }
}
