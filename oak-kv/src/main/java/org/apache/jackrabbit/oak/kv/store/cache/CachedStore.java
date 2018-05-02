package org.apache.jackrabbit.oak.kv.store.cache;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.jackrabbit.oak.kv.store.ID;
import org.apache.jackrabbit.oak.kv.store.Node;
import org.apache.jackrabbit.oak.kv.store.Store;
import org.apache.jackrabbit.oak.kv.store.Value;

public class CachedStore implements Store {

    private final Store store;

    private final Cache<ID, Node> cache;

    public CachedStore(Store store, Cache<ID, Node> cache) {
        this.store = store;
        this.cache = cache;
    }

    @Override
    public ID getTag(String tag) throws IOException {
        return store.getTag(tag);
    }

    @Override
    public void putTag(String tag, ID id) throws IOException {
        store.putTag(tag, id);
    }

    @Override
    public void deleteTag(String tag) throws IOException {
        store.deleteTag(tag);
    }

    @Override
    public Node getNode(ID id) throws IOException {
        try {
            return cache.get(id, () -> store.getNode(id));
        } catch (ExecutionException e) {
            throw (IOException) e.getCause();
        } catch (UncheckedExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    @Override
    public ID putNode(Map<String, Value> properties, Map<String, ID> children) throws IOException {
        return store.putNode(properties, children);
    }

}
