package com.johnsaylor;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class TransactionEmitter {
    BufferedReader reader;
    Producer producer;

    public TransactionEmitter() {
        try {
            this.producer = new Producer();
            this.reader = new BufferedReader(new FileReader("/home/jm/Data/test_PS_20174392719_1491204439457_log.csv"));
            deleteHeader();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        String tx;
        try {
            while((tx = reader.readLine()) != null) {
                producer.send(tx);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteHeader() {
        try {
            reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
