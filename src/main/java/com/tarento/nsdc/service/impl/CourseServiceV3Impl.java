package com.tarento.nsdc.service.impl;

import com.tarento.nsdc.service.ICourseServiceV3;
import com.tarento.nsdc.producer.Producer;
import org.apache.poi.ss.usermodel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

    @Value("${proceed.folder.path}")
    private String proceedFolderPath;

    @Value("${rejected.folder.path}")
    private String rejectedFolderPath;

    @Value("${kafka.v2.topic.name}")
    private String kafkaTopicName;

    private static final Logger logger = LoggerFactory.getLogger(CourseServiceV3Impl.class);

    @Autowired
    private Producer kafkaTemplate;

    public void processIncomingFile(String fileName) {
        String filePath = "/home/manas/CourseBucket/incoming" + File.separator + fileName;
        File file = new File(filePath);
        if (file.exists()) {
            logger.info("Processing file: " + fileName);
            long startTime = System.currentTimeMillis();
            boolean isValid = validateAndProcessExcel(file);
            long endTime = System.currentTimeMillis();
            if (isValid) {
                logger.info("File is valid. Processing completed successfully.");
                moveFile(file, proceedFolderPath, "proceed");
            } else {
                logger.info("File is invalid. Moving to the rejected folder.");
                moveFile(file, rejectedFolderPath, "rejected");
            }
            long timeTaken = endTime - startTime;
            logger.info("Time taken to process the file: " + timeTaken + " milliseconds");
        } else {
            logger.error("File not found: " + fileName);
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
                kafkaTemplate.push(kafkaTopicName, dataMap);
                logger.info("Data sent to Kafka:");
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
                logger.error("Failed to move the file to the " + targetFolderName + " folder: " + file.getName());
            } else {
                logger.info("File moved to the " + targetFolderName + " folder: " + file.getName());
            }
        } catch (IOException e) {
            logger.error("Problem occurred while moving file "+e.getMessage());
        }
    }
}