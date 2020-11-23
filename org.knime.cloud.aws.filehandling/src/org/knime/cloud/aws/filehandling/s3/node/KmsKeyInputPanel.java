/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   2020-09-29 (Alexander Bondaletov): created
 */
package org.knime.cloud.aws.filehandling.s3.node;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.plaf.basic.BasicComboBoxEditor;

import org.knime.cloud.aws.filehandling.s3.AwsUtils;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.util.SwingWorkerWithContext;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;
import software.amazon.awssdk.services.kms.model.AliasListEntry;
import software.amazon.awssdk.services.kms.model.KeyListEntry;
import software.amazon.awssdk.services.kms.model.KmsException;

/**
 * Component for editing KMS keyId setting for S3 SSE-KMS encryption mode. Allows user to enter keyId manually or query
 * available keys from AWS and select one on them.
 *
 * @author Alexander Bondaletov
 */
public class KmsKeyInputPanel extends JPanel {
    private static final long serialVersionUID = 1L;

    private static final NodeLogger LOGGER = NodeLogger.getLogger(KmsKeyInputPanel.class);

    private final transient SettingsModelString m_kmsKeyId;

    private final JLabel m_keyIdLabel = new JLabel("KMS key id");
    private CloudConnectionInformation m_connInfo;

    private final DefaultComboBoxModel<KeyItem> m_comboModel;
    private final JComboBox<KeyItem> m_combobox;
    private final JButton m_fetchBtn;
    private final JButton m_cancelBtn;

    private transient ListKeysWorker m_fetchWorker;


    /**
     * @param kmsKeyId {@link SettingsModelString} holding KMS keyId.
     *
     */
    public KmsKeyInputPanel(final SettingsModelString kmsKeyId) {
        m_kmsKeyId = kmsKeyId;

        m_comboModel = new DefaultComboBoxModel<>(new KeyItem[]{});
        m_combobox = new JComboBox<>(m_comboModel);
        m_combobox.setEditable(true);
        m_combobox.setEditor(new KeyItemComboBoxEditor(m_combobox));
        m_combobox.addActionListener(e -> onSelectionChanged());

        m_fetchBtn = new JButton("List keys");
        m_fetchBtn.addActionListener(e -> onFetch());

        m_cancelBtn = new JButton("Cancel");
        m_cancelBtn.addActionListener(e -> {
            if (m_fetchWorker != null) {
                m_fetchWorker.cancel(true);
            }
        });
        m_cancelBtn.setVisible(false);
        m_cancelBtn.setPreferredSize(m_fetchBtn.getPreferredSize());

        initUI();
    }

    private void initUI() {
        GridBagConstraints c = new GridBagConstraints();
        setLayout(new GridBagLayout());
        c.gridx = 0;
        c.gridy = 0;
        c.anchor = GridBagConstraints.LINE_START;
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0;
        c.insets = new Insets(0, 0, 0, 5);

        add(m_keyIdLabel, c);
        c.gridx += 1;

        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 0.5;
        add(m_combobox, c);

        c.fill = GridBagConstraints.NONE;
        c.anchor = GridBagConstraints.LINE_END;
        c.gridx += 1;
        c.weightx = 0;
        add(m_fetchBtn, c);

        c.gridx += 1;
        add(m_cancelBtn, c);
    }

    private void onSelectionChanged() {
        KeyItem item = (KeyItem)m_comboModel.getSelectedItem();
        m_kmsKeyId.setStringValue(item.getKeyId());
    }

    private void onFetch() {
        if (m_connInfo == null) {
            return;
        }

        m_fetchWorker = new ListKeysWorker();
        m_fetchWorker.listKeys();
    }

    /**
     * Method intended to be called after settings are loaded.
     *
     * @param connInfo {@link CloudConnectionInformation} object.
     */
    public void onSettingsLoaded(final CloudConnectionInformation connInfo) {
        m_connInfo = connInfo;
        m_comboModel.setSelectedItem(new KeyItem(m_kmsKeyId.getStringValue()));
    }

    @Override
    public void setEnabled(final boolean enabled) {
        super.setEnabled(enabled);

        m_keyIdLabel.setEnabled(enabled);
        m_combobox.setEnabled(enabled);
        m_fetchBtn.setEnabled(enabled);
    }

    private class ListKeysWorker extends SwingWorkerWithContext<List<KeyItem>, Void> {
        @Override
        protected List<KeyItem> doInBackgroundWithContext() throws Exception {
            return fetchKmsKeys();
        }

        @Override
        protected void doneWithContext() {
            m_cancelBtn.setVisible(false);
            m_fetchBtn.setVisible(true);

            if (isCancelled()) {
                return;
            }

            try {
                onKeysLoaded(get());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException ex) {
                showError(ex);
            }
        }

        public void listKeys() {
            m_cancelBtn.setVisible(true);
            m_fetchBtn.setVisible(false);

            execute();
        }

        private void showError(final ExecutionException ex) {
            String message = ex.getMessage();
            Throwable cause = ex.getCause();

            if (cause != null) {
                message = cause.getMessage();

                if (cause instanceof KmsException) {
                    KmsException kmsEx = (KmsException)cause;
                    if (kmsEx.awsErrorDetails() != null) {
                        message = kmsEx.awsErrorDetails().errorMessage();
                    }
                }
            }

            LOGGER.warn(message, cause != null ? cause : ex);
            JOptionPane.showMessageDialog(getRootPane(), message, "Error", JOptionPane.ERROR_MESSAGE);
        }

        private List<KeyItem> fetchKmsKeys() {
            KmsClientBuilder builder = KmsClient.builder()//
                .region(Region.of(m_connInfo.getHost()))//
                .credentialsProvider(AwsUtils.getCredentialProvider(m_connInfo));

            try (KmsClient client = builder.build()) {
                List<KeyListEntry> keys = client.listKeys().keys();
                Map<String, String> aliases = fetchAliases(client);

                return keys.stream().map(key -> new KeyItem(key.keyId(), aliases.get(key.keyId())))
                    .collect(Collectors.toList());
            }
        }

        private Map<String, String> fetchAliases(final KmsClient client) {
            try {
                Map<String, String> result = new HashMap<>();

                List<AliasListEntry> aliases = client.listAliases().aliases();
                for (AliasListEntry alias : aliases) {
                    if (alias.targetKeyId() != null) {
                        result.put(alias.targetKeyId(), alias.aliasName());
                    }
                }

                return result;
            } catch (AwsServiceException ex) {
                LOGGER.warn(ex.getMessage(), ex);
            }
            return Collections.emptyMap();
        }

        private void onKeysLoaded(final List<KeyItem> keys) {
            for (KeyItem key : keys) {
                if (m_comboModel.getIndexOf(key) == -1) {
                    m_comboModel.addElement(key);
                }
            }
        }
    }

    private static class KeyItem {
        private String m_keyId;

        private String m_alias;

        public KeyItem(final String keyId) {
            this(keyId, null);
        }

        public KeyItem(final String keyId, final String alias) {
            m_keyId = keyId;
            m_alias = alias;
        }

        public String getKeyId() {
            return m_keyId;
        }

        @Override
        public String toString() {
            if (m_alias != null && !m_alias.isEmpty()) {
                return String.format("%s (%s)", m_alias, m_keyId);
            }
            return m_keyId;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj != null && this.getClass() == obj.getClass()) {
                return m_keyId.equals(((KeyItem)obj).m_keyId);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return m_keyId.hashCode();
        }
    }

    private static class KeyItemComboBoxEditor extends BasicComboBoxEditor {

        private final JComboBox<KeyItem> m_parent;

        private boolean m_ignoreListeners;

        public KeyItemComboBoxEditor(final JComboBox<KeyItem> parent) {
            m_parent = parent;
            m_ignoreListeners = false;

            editor.getDocument().addDocumentListener(new DocumentListener() {
                @Override
                public void removeUpdate(final DocumentEvent e) {
                    onTextFieldUpdated();
                }

                @Override
                public void insertUpdate(final DocumentEvent e) {
                    onTextFieldUpdated();
                }

                @Override
                public void changedUpdate(final DocumentEvent e) {
                    onTextFieldUpdated();
                }
            });
        }

        private void onTextFieldUpdated() {
            if (!m_ignoreListeners) {
                m_parent.setSelectedItem(getItem());
            }
        }

        @Override
        public void setItem(final Object anObject) {
            m_ignoreListeners = true;
            super.setItem(anObject);
            m_ignoreListeners = false;
        }

        @Override
        public KeyItem getItem() {
            Object newVal = super.getItem();
            if (!(newVal instanceof KeyItem)) {
                return new KeyItem(newVal.toString());
            }
            return (KeyItem)newVal;
        }
    }
}
