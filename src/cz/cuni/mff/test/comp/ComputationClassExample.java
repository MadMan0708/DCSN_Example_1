/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.cuni.mff.test.comp;

import cz.cuni.mff.bc.api.main.ITask;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author Aku
 */
public class ComputationClassExample implements ITask {

    Integer[] data;
    long timeout = 600000;

    @Override
    public void loadData(Path nameOfTheFile) {
        // Each class can have arbitrary name

        ArrayList<Integer> numbers = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(nameOfTheFile.toFile())))) {

            while (dis.available() != 0) {
                numbers.add(dis.readInt());
            }
            data = (Integer[]) numbers.toArray(new Integer[numbers.size()]);
        } catch (IOException e) {
        }
    }

    @Override
    public void calculate() {
        Arrays.sort(data);
        try {
            this.wait(timeout); // wait to make task look like they last for {timeout} miliseconds
        } catch (InterruptedException e) {
        }

    }

    @Override
    public void saveData(Path nameOfTheFile) {

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(nameOfTheFile.toFile()))) {
            for (int i = 0; i < data.length; i++) {
                dos.writeInt(data[i]);
            }
        } catch (IOException e) {
        }
    }
}
