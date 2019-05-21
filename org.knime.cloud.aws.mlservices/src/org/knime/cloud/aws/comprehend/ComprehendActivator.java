package org.knime.cloud.aws.comprehend;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;

/**
 * The activator class controls the plug-in life cycle
 */
public class ComprehendActivator extends AbstractUIPlugin {
	// The shared instance
	private static ComprehendActivator plugin;

	/**
	 * The constructor.
	 */
	public ComprehendActivator() {
	}

	/**
	 * This method is called when the plug-in is stopped.
	 * 
	 * @param context The bundle context.
	 * @throws Exception If cause by super class.
	 */
	@Override
	public void stop(final BundleContext context) throws Exception {
		plugin = null;
		super.stop(context);
	}

	/**
	 * Returns an image descriptor for the image file at the given plug-in relative
	 * path.
	 *
	 * @param path the path
	 * @return the image descriptor
	 */
	public static ImageDescriptor getImageDescriptor(final String path) {
		return AbstractUIPlugin.imageDescriptorFromPlugin("org.knime.cloud.aws.comprehend", path);
	}

	/**
	 * Returns the shared instance
	 *
	 * @return the shared instance
	 */
	public static ComprehendActivator getDefault() {
		return plugin;
	}
	
    /**
     * Resolves a path relative to the plug-in or any fragment's root into an absolute path.
     *
     * @param relativePath a relative path
     * @return the resolved absolute path
     * @since 3.3
     */
    public static File resolvePath(final String relativePath) {
        final Bundle myself = FrameworkUtil.getBundle(ComprehendActivator.class);
        try {
            final URL fileUrl = FileLocator.toFileURL(FileLocator.find(myself, new Path(relativePath), null));
            return new File(fileUrl.getPath());
        } catch (final IOException ex) {
            NodeLogger.getLogger(ComprehendActivator.class)
                .error("Could not resolve relativ path '" + relativePath + "': " + ex.getMessage(), ex);
            return new File("");
        }
    }
}
