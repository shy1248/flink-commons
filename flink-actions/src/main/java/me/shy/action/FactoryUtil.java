package me.shy.action;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shy
 * @date 2023/09/03 04:58
 **/
public class FactoryUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FactoryUtil.class);
    
    public static ActionFactory discoverActionFactory(ClassLoader classLoader, String name) {
        List<ActionFactory> foundServices = discoverServices(classLoader);
        List<ActionFactory> matchedServices = foundServices.stream()
                .filter(f -> f.name().equals(name))
                .collect(Collectors.toList());
        if(matchedServices.size() != 1) {
            throw new FactoryException(String.format("Error number services for '%s'." 
                            + " expected 1, but %s found.", name, matchedServices.size()));
        }
        // only needed this first one
        return matchedServices.get(0);
    }
    
    public static List<String> discoverActionNames(ClassLoader classLoader) {
        return discoverServices(classLoader).stream()
                .map(ActionFactory::name)
                .collect(Collectors.toList());
    }
    
    private static List<ActionFactory> discoverServices(ClassLoader classLoader) {
        final Iterator<ActionFactory> iterator = 
                ServiceLoader.load(ActionFactory.class, classLoader).iterator();
        final List<ActionFactory> foundServices = new ArrayList<>();
        while(true) {
            try {
                if (!iterator.hasNext()) {
                    break;
                }
                foundServices.add(iterator.next());
            } catch (Throwable t) {
                if (t instanceof NoClassDefFoundError) {
                    LOG.debug(String.format("NoClassDefFoundError when loading service %s. " 
                            + "This is expected when try to loading factory, but no implementation found.",
                            ActionFactory.class.getCanonicalName()));
                } else {
                    throw new RuntimeException("Unexpected error when trying to load service provider.", t);
                }
            }
        }
        return foundServices;
    }
}
