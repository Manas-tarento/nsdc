package com.tarento.nsdc.service.impl;

import com.tarento.nsdc.producer.Producer;
import com.tarento.nsdc.service.ICourseService;
import org.apache.poi.ss.usermodel.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class CourseServiceImpl {

    private static final String PROCEED_FOLDER_PATH = "/home/manas/CourseBucket/proceed";
    private static final String REJECTED_FOLDER_PATH = "/home/manas/CourseBucket/rejected";
    private static final String KAFKA_TOPIC_NAME = "course";

    @Autowired
    private Producer kafkaTemplate;

    public void processIncomingFile(String fileName) {
        String filePath = "/home/manas/CourseBucket/incoming" + File.separator + fileName;
        File file = new File(filePath);
        if (file.exists()) {
            System.out.println("Processing file: " + fileName);
            boolean isValid = validateAndMoveExcel(file);
            if (isValid) {
                System.out.println("File is valid. Processing completed successfully.");
            } else {
                System.out.println("File is invalid. Moving to the rejected folder.");
            }
        } else {
            System.out.println("File not found: " + fileName);
        }
    }

    private boolean validateAndMoveExcel(File file) {
        try (FileInputStream fis = new FileInputStream(file);
             Workbook workbook = WorkbookFactory.create(fis)) {
            Sheet sheet = workbook.getSheetAt(0);
            Row headerRow = sheet.getRow(0);
            int rowCount = sheet.getLastRowNum();
            for (int rowIndex = 1; rowIndex <= rowCount; rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (row != null) {
                    boolean isValid = validateRow(row);
                    if (!isValid) {
                        moveFile(file, REJECTED_FOLDER_PATH, "rejected");
                        return false;
                    }
                }
            }
            sendDataToKafka(file, headerRow);
            moveFile(file, PROCEED_FOLDER_PATH, "proceed");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean validateRow(Row row) {
        DataFormatter formatter = new DataFormatter();
        for (Cell cell : row) {
            String cellValue = formatter.formatCellValue(cell).trim();
            if (cellValue.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private void moveFile(File file, String targetFolderPath, String targetFolderName) {
        try {
            Path sourceFilePath = file.toPath();
            Path targetFolderPathWithFile = Paths.get(targetFolderPath, file.getName());
            Files.move(sourceFilePath, targetFolderPathWithFile);
            if (Files.exists(sourceFilePath)) {
                System.out.println("Failed to move the file to the " + targetFolderName + " folder: " + file.getName());
            } else {
                System.out.println("File moved to the " + targetFolderName + " folder: " + file.getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void sendDataToKafka(File file, Row headerRow) {
        List<Map<String, String>> dataList = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(file);
             Workbook workbook = WorkbookFactory.create(fis)) {
            Sheet sheet = workbook.getSheetAt(0);
            int rowCount = sheet.getLastRowNum();
            for (int rowIndex = 1; rowIndex <= rowCount; rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (row != null) {
                    Map<String, String> dataMap = convertRowToDataMap(row, headerRow);
                    dataList.add(dataMap);
                }
            }
            dataList.forEach(data -> kafkaTemplate.push(KAFKA_TOPIC_NAME, data));
            dataList.forEach(data -> System.out.println("values which has been sent to kafka " + data.toString()));
        } catch (Exception e) {
            System.out.println("Failed to push");
        }
    }

    private Map<String, String> convertRowToDataMap(Row row, Row headerRow) {
        Map<String, String> dataMap = new HashMap<>();
        DataFormatter formatter = new DataFormatter();
        for (int colIndex = 0; colIndex < headerRow.getLastCellNum(); colIndex++) {
            Cell headerCell = headerRow.getCell(colIndex);
            Cell cell = row.getCell(colIndex);
            if (headerCell != null && cell != null) {
                String propertyName = headerCell.getStringCellValue().trim();
                String cellValue = formatter.formatCellValue(cell).trim();
                dataMap.put(propertyName, cellValue);
                dataMap.put("id", UUID.randomUUID().toString());
            }
        }
        return dataMap;
    }
}
