package com.tarento.nsdc.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tarento.nsdc.producer.Producer;
import com.tarento.nsdc.service.ICourseServiceV2;
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
import java.util.stream.Collectors;

@Service
public class CourseServiceImplV2 implements ICourseServiceV2 {

    @Value("${proceed.folder.path}")
    private String proceedFolderPath;

    @Value("${rejected.folder.path}")
    private String rejectedFolderPath;

    @Value("${kafka.topic.name}")
    private String kafkaTopicName;

    private static final Logger logger = LoggerFactory.getLogger(CourseServiceImplV2.class);

    @Autowired
    private Producer kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

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
            int numberOfSheets = workbook.getNumberOfSheets();
            for (int sheetIndex = 0; sheetIndex < numberOfSheets; sheetIndex++) {
                Sheet sheet = workbook.getSheetAt(sheetIndex);
                int lastRowIndex = sheet.getLastRowNum();
                if (lastRowIndex >= 1) {
                    Row headerRow = sheet.getRow(0);
                    Row dataRow = sheet.getRow(1);
                    if (headerRow != null && dataRow != null) {
                        boolean isValidHeader = validateRow(headerRow);
                        boolean isValidData = validateRow(dataRow);
                        if (!isValidData) {
                            logger.error("Validation Failed might be cell is empty");
                            return false;
                        }
                        Map<String, Object> dataMap = convertRowToDataMap(dataRow, headerRow);
                        dataMap.put("id", UUID.randomUUID().toString());
                        kafkaTemplate.push(kafkaTopicName, dataMap);
                        logger.info("Data sent to Kafka for sheet: " + sheet.getSheetName());
                    }
                }
            }
            return true;
        } catch (IOException e) {
            logger.error("Validation failed");
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
                logger.error("Failed to move the file to the " + targetFolderName + " folder: " + file.getName());
            } else {
                logger.info("File moved to the " + targetFolderName + " folder: " + file.getName());
            }
        } catch (IOException e) {
            logger.error("Exception occurred while moving file");
        }
    }

    private Map<String, Object> convertRowToDataMap(Row row, Row headerRow) {
        Map<String, Object> dataMap = new HashMap<>();
        DataFormatter formatter = new DataFormatter();
        Map<String, String> headerToPropertyMapping = generateHeaderToPropertyMapping();
        for (int colIndex = 0; colIndex < headerRow.getLastCellNum(); colIndex++) {
            Cell headerCell = headerRow.getCell(colIndex);
            Cell cell = row.getCell(colIndex);
            if (headerCell != null && cell != null) {
                String excelHeader = headerCell.getStringCellValue().trim();
                String propertyName = headerToPropertyMapping.get(excelHeader);
                if (propertyName != null) {
                    String cellValue = formatter.formatCellValue(cell).trim();
                    if (cellValue.equalsIgnoreCase("Yes") || cellValue.equalsIgnoreCase("Y")) {
                        dataMap.put(propertyName, true);
                    } else if (cellValue.equalsIgnoreCase("No") || cellValue.equalsIgnoreCase("N")) {
                        dataMap.put(propertyName, false);
                    } else if (propertyName.equals("tools")) {
                        String[] values = cellValue.split(",");
                        List<String> valueList = Arrays.stream(values).map(String::trim).collect(Collectors.toList());
                        dataMap.put(propertyName, valueList);
                    } else if (propertyName.equals("faqs")) {
                        Map<String, String> faqsMap = new HashMap<>();
                        String[] faqsArray = cellValue.split("\\d+\\.");
                        for (int i = 1; i < faqsArray.length; i++) {
                            String faq = faqsArray[i].trim();
                            String[] faqParts = faq.split("Ans:");
                            if (faqParts.length == 2) {
                                String question = faqParts[0].trim();
                                String answer = faqParts[1].trim();
                                faqsMap.put(question, answer);
                            }
                        }
                        dataMap.put(propertyName, faqsMap);
                    } else {
                        String trimmedValue = cellValue.trim().replace("\n", "");
                        dataMap.put(propertyName, trimmedValue);
                    }
                }
            }
        }
        return dataMap;
    }

    private Map<String, String> generateHeaderToPropertyMapping() {
        Map<String, String> mapping = new HashMap<>();
        try {
            File jsonFile = new File("/home/manas/POC/nsdc/headerToPropertyMapping.json");
            mapping = objectMapper.readValue(jsonFile, Map.class);
        } catch (IOException e) {
            logger.error("Error while doing header mapping"+ e.getMessage());
        }
        return mapping;
    }
}