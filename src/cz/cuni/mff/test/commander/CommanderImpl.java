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
 *
 * @author Jakub
 */
public class CommanderImpl extends Commander {

    private static StandardRemoteProvider apiWithLog;
    private static RemoteProvider apiWithoutLog;
    private static final Logger LOG = Logger.getLogger(Commander.class.getName());
    private String projectName;

    private static File createBigFile() throws IOException {
        File tmp = File.createTempFile("velky_balik_dat", ".txt");
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(tmp))) {
            Random rand = new Random();
            for (int k = 0; k < 4; k++) {
                for (int j = 0; j <= 99999; j++) {
                    dos.writeInt(rand.nextInt(10000));
                }
            }
        } catch (IOException e) {
            throw new IOException("Obrovsky soubor nemohl byt vytvoren", e);
        }
        return tmp;
    }

    // rozdeli soubor bigFile na n souboru
    private File splitFileToNFiles(File bigFile, int n) throws IOException {
        File tmpDir = Files.createTempDirectory("rozdelene_soubory").toFile();
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

    //slouceni
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
        this.apiWithLog = apiWithLog;
        apiWithoutLog = apiWithLog.getRemoteProvider();
        try {
            projectName = JarTools.getAttributeFromManifest(apiWithLog.getCurrentJarPath(), "Project-Name");
            File velkyBalik = createBigFile();
            LOG.log(Level.INFO, "Obrovsky soubor vytvoren");
            File tmpDir = splitFileToNFiles(velkyBalik, 10);
            LOG.log(Level.INFO, "Obrovsky soubor rozdelen");
            //File zipped = File.createTempFile("data", ".zip");
            File zipped = new File(apiWithLog.getStandartDownloadDir().toFile(), "data.zip");
            CustomIO.zipFiles(zipped, tmpDir.listFiles());
            LOG.log(Level.INFO, "Data zabalena do zip archivu");
            apiWithLog.uploadProject(apiWithLog.getCurrentJarPath(), zipped.toPath());
            LOG.log(Level.INFO, "Data odeslana na server");
            while (!apiWithoutLog.isProjectReadyForDownload(projectName)) {
                try {
                    Thread.sleep(1000);
                    apiWithLog.printProjectInfo(projectName);
                } catch (InterruptedException e) {
                    LOG.info("Interrupted during waiting for project to be complete");
                }
            }
            LOG.log(Level.INFO, "Vypocet dokoncen");
            File poVypoctu = File.createTempFile("poVypoctu", ".zip");
            apiWithLog.download(projectName, poVypoctu.toPath());
            LOG.log(Level.INFO, "Vypoctena data stazena ze serveru");
            File poVypoctuDir = Files.createTempDirectory("_poVypoctyDir").toFile();
            CustomIO.extractZipFile(poVypoctu, poVypoctuDir);
            LOG.log(Level.INFO, "Vypoctena data rozbalena");
            File finalni = new File(apiWithLog.getStandartDownloadDir().toFile(), projectName + ".txt");
            mergeFiles(poVypoctuDir.listFiles(), finalni);
            LOG.log(Level.INFO, "Finalni soubor je na adrese: {0}", finalni.getAbsolutePath());
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Chyba: {0}", e.getMessage());
        }
    }
}
