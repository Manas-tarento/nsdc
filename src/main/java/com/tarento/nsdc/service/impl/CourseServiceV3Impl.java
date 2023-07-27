package com.tarento.nsdc.service.impl;

import com.tarento.nsdc.service.ICourseServiceV3;
import com.tarento.nsdc.producer.Producer;
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

@Service
public class CourseServiceV3Impl implements ICourseServiceV3 {

    private static final String PROCEED_FOLDER_PATH = "/home/manas/CourseBucket/proceed";
    private static final String REJECTED_FOLDER_PATH = "/home/manas/CourseBucket/rejected";
    private static final String KAFKA_TOPIC_NAME = "courseV2";

    @Autowired
    private Producer kafkaTemplate;

   /* public void processIncomingFile(String fileName) {
        String filePath = "/home/manas/CourseBucket/incoming" + File.separator + fileName;
        File file = new File(filePath);
        if (file.exists()) {
            System.out.println("Processing file: " + fileName);
            boolean isValid = validateAndProcessExcel(file);
            if (isValid) {
                System.out.println("File is valid. Processing completed successfully.");
                moveFile(file, PROCEED_FOLDER_PATH, "proceed");
            } else {
                System.out.println("File is invalid. Moving to the rejected folder.");
                moveFile(file, REJECTED_FOLDER_PATH, "rejected");
            }
        } else {
            System.out.println("File not found: " + fileName);
        }
    }*/

    public void processIncomingFile(String fileName) {
        String filePath = "/home/manas/CourseBucket/incoming" + File.separator + fileName;
        File file = new File(filePath);
        if (file.exists()) {
            System.out.println("Processing file: " + fileName);
            long startTime = System.currentTimeMillis(); // Record the start time
            boolean isValid = validateAndProcessExcel(file);
            long endTime = System.currentTimeMillis(); // Record the end time

            if (isValid) {
                System.out.println("File is valid. Processing completed successfully.");
                moveFile(file, PROCEED_FOLDER_PATH, "proceed");
            } else {
                System.out.println("File is invalid. Moving to the rejected folder.");
                moveFile(file, REJECTED_FOLDER_PATH, "rejected");
            }

            long timeTaken = endTime - startTime;
            System.out.println("Time taken to process the file: " + timeTaken + " milliseconds");
        } else {
            System.out.println("File not found: " + fileName);
        }
    }

    private boolean validateAndProcessExcel(File file) {
        try (FileInputStream fis = new FileInputStream(file);
             Workbook workbook = WorkbookFactory.create(fis)) {
            for (int sheetIndex = 0; sheetIndex < workbook.getNumberOfSheets(); sheetIndex++) {
                Sheet sheet = workbook.getSheetAt(sheetIndex);
                Row headerRow = sheet.getRow(0);
                if (headerRow == null) {
                    return false;
                }
                Map<String, Object> dataMap = convertSheetToDataMap(sheet);
                kafkaTemplate.push(KAFKA_TOPIC_NAME, dataMap);
                System.out.println("Data sent to Kafka:");
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private Map<String, Object> convertSheetToDataMap(Sheet sheet) {
        Map<String, Object> dataMap = new HashMap<>();
        List<Map<String, String>> rows = new ArrayList<>();
        DataFormatter formatter = new DataFormatter();
        Iterator<Row> rowIterator = sheet.iterator();
        Row headerRow = rowIterator.next();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Map<String, String> rowData = new HashMap<>();
            if (isEmptyRow(row)) {
                continue;
            }
            for (int colIndex = 0; colIndex < headerRow.getLastCellNum(); colIndex++) {
                Cell headerCell = headerRow.getCell(colIndex);
                Cell cell = row.getCell(colIndex);
                if (headerCell != null && cell != null) {
                    String excelHeader = headerCell.getStringCellValue().trim();
                    String cellValue = formatter.formatCellValue(cell).trim();
                    rowData.put(excelHeader, cellValue);
                }
            }
            rows.add(rowData);
        }
        dataMap.put("jsonData", rows);
        dataMap.put("id", sheet.getSheetName());
        return dataMap;
    }

    private boolean isEmptyRow(Row row) {
        Iterator<Cell> cellIterator = row.cellIterator();
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            if (cell != null && cell.getCellType() != CellType.BLANK) {
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

 /*   private String validateRows(Map<String, Object> dataMap) {
        for (String sheetName : dataMap.keySet()) {
            List<Map<String, String>> rows = (List<Map<String, String>>) dataMap.get(sheetName);
            for (int i = 0; i < rows.size(); i++) {
                Map<String, String> row = rows.get(i);
                for (String key : row.keySet()) {
                    String value = row.get(key);
                    if (value == null || value.trim().isEmpty()) {
                        return "Empty value found in " + sheetName + " at row " + (i + 1) + ", column " + key;
                    }
                }
            }
        }
        return "Valid";
    }*/
}
