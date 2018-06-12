package org.knime.cloud.google.util;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.cloud.core.util.port.CloudConnectionInformationPortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 *
 * @author Jason Tyler, KNIME.com
 */
public class GoogleDriveConnectionInformationPortObject extends ConnectionInformationPortObject {

    /**
     * @noreference This class is not intended to be referenced by clients.
     */
    public static final class Serializer
        extends AbstractSimplePortObjectSerializer<GoogleDriveConnectionInformationPortObject> {
    }

    /**
     * No-argument constructor for framework
     */
    public GoogleDriveConnectionInformationPortObject() {
    }

    /**
     * Type of this port.
     */
    public static final PortType TYPE = ConnectionInformationPortObject.TYPE;

    /**
     * Type of this optional port.
     */
    public static final PortType TYPE_OPTIONAL = ConnectionInformationPortObject.TYPE_OPTIONAL;

    /**
     * Creates a port object with the given connection information.
     *
     * @param connectionInformationPOS The spec wrapping the connection information.
     */
    public GoogleDriveConnectionInformationPortObject(
        final CloudConnectionInformationPortObjectSpec connectionInformationPOS) {
        super(connectionInformationPOS);
    }

}
