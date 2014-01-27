/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.cuni.mff.test.commander;

import cz.cuni.mff.bc.api.main.Commander;
import cz.cuni.mff.bc.api.main.CustomIO;
import cz.cuni.mff.bc.api.main.JarTools;
import cz.cuni.mff.bc.api.main.RemoteProvider;
import cz.cuni.mff.bc.api.main.StandardRemoteProvider;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Commander class implementation
 *
 * @author Jakub Hava
 */
public class CommanderImpl extends Commander {

    private static RemoteProvider apiWithoutLog;
    private static final Logger LOG = Logger.getLogger(Commander.class.getName());
    private String projectName;

    private static File createBigFile() throws IOException {
        File tmp = File.createTempFile("big_data_file", ".txt");
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(tmp))) {
            Random rand = new Random();
            for (int k = 0; k < 4; k++) {
                for (int j = 0; j <= 99999; j++) {
                    dos.writeInt(rand.nextInt(10000));
                }
            }
        } catch (IOException e) {
            throw new IOException("Data file coulnd't be created", e);
        }
        return tmp;
    }

    // splits the bigFile to n files
    private File splitFileToNFiles(File bigFile, int n) throws IOException {
        File tmpDir = Files.createTempDirectory("splited_files").toFile();
        long averageLength = bigFile.length() / (long) n;
        long avTemp;
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(bigFile)))) {
            for (int i = 0; i < n; i++) {
                File tmpFile = new File(tmpDir, "data_" + i + ".txt");
                try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(tmpFile))) {
                    avTemp = 0;
                    while (avTemp != averageLength && dis.available() != 0) {
                        dos.writeInt(dis.readInt());
                        avTemp = avTemp + 4;
                    }
                } catch (IOException e) {
                    throw new IOException("Coudln't create file:" + tmpFile.getName(), e);
                }
            }
            return tmpDir;
        } catch (IOException e) {
            throw new IOException("Coudln't create tmp dir:" + tmpDir.getName(), e);
        }
    }

    //merging
    private File mergeFiles(File[] filePaths, File output) throws IOException {
        DataInputStream[] readerArray = new DataInputStream[filePaths.length];
        for (int index = 0; index < readerArray.length; index++) {
            readerArray[index] = new DataInputStream(new FileInputStream(filePaths[index]));
        }
        boolean[] closedReaderFlag = new boolean[readerArray.length];

        PrintWriter writer = new PrintWriter(output);
        int currentReaderIndex = -1;
        DataInputStream currentReader = null;
        int currentInt;
        while (getNumberReaderClosed(closedReaderFlag) < readerArray.length) {
            currentReaderIndex = (currentReaderIndex + 1) % readerArray.length;

            if (closedReaderFlag[currentReaderIndex]) {
                continue;
            }

            currentReader = readerArray[currentReaderIndex];
            if (currentReader.available() == 0) {
                currentReader.close();
                closedReaderFlag[currentReaderIndex] = true;
                continue;
            } else {
                currentInt = currentReader.readInt();
                writer.println(currentInt);
            }
        }
        writer.close();
        for (int index = 0; index < readerArray.length; index++) {
            if (!closedReaderFlag[index]) {
                readerArray[index].close();
            }
        }
        return output;

    }

    private static int getNumberReaderClosed(boolean[] closedReaderFlag) {
        int count = 0;
        for (boolean currentFlag : closedReaderFlag) {
            if (currentFlag) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void start(StandardRemoteProvider apiWithLog) {
        LOG.setParent(apiWithLog.getLogger());
        apiWithoutLog = apiWithLog.getRemoteProvider();
        try {
            projectName = JarTools.getAttributeFromManifest(apiWithLog.getCurrentJarPath(), "Project-Name");
            File bigFile = createBigFile();
            LOG.log(Level.INFO, "Big data file created");
            File tmpDir = splitFileToNFiles(bigFile, 10);
            LOG.log(Level.INFO, "Big data file splitted");
            File zipped = new File(apiWithLog.getStandartDownloadDir().toFile(), "data.zip");
            CustomIO.zipFiles(zipped, tmpDir.listFiles());
            LOG.log(Level.INFO, "Data packed into the zip archive");
            apiWithLog.uploadProject(apiWithLog.getCurrentJarPath(), zipped.toPath());
            LOG.log(Level.INFO, "Data sent to the server");
            while (!apiWithoutLog.isProjectReadyForDownload(projectName)) {
                try {
                    Thread.sleep(5000); // check each 5 seconds
                    apiWithLog.printProjectInfo(projectName);
                } catch (InterruptedException e) {
                    LOG.info("Interrupted during waiting for project to be complete");
                }
            }
            LOG.log(Level.INFO, "Task computation finished");
            File download = File.createTempFile("afterComp", ".zip");
            apiWithLog.download(projectName, download.toPath());
            LOG.log(Level.INFO, "Data downloaded from the server");
            File extractionDir = Files.createTempDirectory("extractionDir").toFile();
            CustomIO.extractZipFile(download, extractionDir);
            LOG.log(Level.INFO, "Data unpacked");
            File output = new File(apiWithLog.getStandartDownloadDir().toFile(), projectName + ".txt");
            mergeFiles(extractionDir.listFiles(), output);
            LOG.log(Level.INFO, "Output file is located at : {0}", output.getAbsolutePath());
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Error: {0}", e.getMessage());
        }
    }
}
