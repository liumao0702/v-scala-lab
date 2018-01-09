package engineer.zhangwei.storage.geode.util;

import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.pdx.PdxInstance;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Set;

import static javax.management.ObjectName.getInstance;

/**
 * Created by Zhangwei on 2017/4/17.
 *
 */
public class GeodeUtil {
    private static volatile GeodeUtil geodeUtil;
    private volatile Region<String,PdxInstance> region ;
    private ClientCache clientCache ;
    private GeodeUtil(){
        if(clientCache == null)    {
            clientCache = GeodeCache.getCache();
        }
    }
    public static GeodeUtil getInstance(){
        if (geodeUtil ==null)  {
            synchronized (GeodeUtil.class)  {
                if (geodeUtil ==null)  {
                    geodeUtil =new GeodeUtil();
                }
            }
        }
        return geodeUtil;

    }
    public synchronized Region<String,PdxInstance> getRegion (String regionName){
        if(region==null)
            region = clientCache.<String, PdxInstance>createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
        else if (!region.getName().equals(regionName)) {
            region.close();
            region = clientCache.<String, PdxInstance>createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
        }
        return this.region;
    }
//    public static Boolean ping()  {
//        try {
//            JMXServiceURL serviceURL =
//                    new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + GeodeConfig.HOST + "/jmxrmi");
//            JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceURL, null);
//            MBeanServerConnection connection = jmxConnector.getMBeanServerConnection();
//            String[] geodeServers = (String[]) connection.invoke(ObjectName.getInstance("GemFire:service=System,type=Distributed"), "listServers", null, null);
//            for (String str : geodeServers) {
//                //System.out.println(str);
//            }
//            return geodeServers.length > 0;
//        }
//        catch (Exception ex)    {
//            return false;
//        }
//    }

    public void closeClientCache(){
        if (this.clientCache!=null) this.clientCache.close();
    }
    public Object query(String queryString) throws NameResolutionException, TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {
        if (this.clientCache!=null) {
            if(!clientCache.isClosed())
                return this.clientCache.getQueryService().newQuery(queryString).execute();
            else {
                clientCache=GeodeCache.getCache();
                return this.clientCache.getQueryService().newQuery(queryString).execute();
            }
        }
        else
            throw new RuntimeException("Client Cache init failed!...");
    }
    public void deleteByKeys(Collection<String> keys)  {
        if (this.region!=null&&!keys.isEmpty()) this.region.removeAll(keys);
    }
    public  void deleteByQueryKeys(String queryString)  {
        if (this.clientCache!=null) {
            Collection<String> keys ;
            try {
                keys = (Collection<String>) this.clientCache.getQueryService().newQuery(queryString).execute();
                if (this.region!=null&&!keys.isEmpty()) {
                    this.region.removeAll(keys);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

