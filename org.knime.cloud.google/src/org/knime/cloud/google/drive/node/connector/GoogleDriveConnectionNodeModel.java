package org.knime.cloud.google.drive.node.connector;

import java.io.File;
import java.io.IOException;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortType;


/**
*
* @author Jason Tyler, KNIME.com
*/
public class GoogleDriveConnectionNodeModel extends NodeModel {
	
	// the logger instance
    private static final NodeLogger logger = NodeLogger
            .getLogger(GoogleDriveConnectionNodeModel.class);

    
    public GoogleDriveConnectionNodeModel() {
    	super(0, 1);
    }
//	public GoogleDriveConnectionNodeModel(int nrInDataPorts, int nrOutDataPorts) {
//		super(nrInDataPorts, nrOutDataPorts);
//		// TODO Auto-generated constructor stub
//	}
//
//	public GoogleDriveConnectionNodeModel(PortType[] inPortTypes, PortType[] outPortTypes) {
//		super(inPortTypes, outPortTypes);
//		// TODO Auto-generated constructor stub
//	}

	@Override
	protected void loadInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void saveInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void saveSettingsTo(NodeSettingsWO settings) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void reset() {
		// TODO Auto-generated method stub

	}

}
