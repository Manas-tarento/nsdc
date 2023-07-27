package com.tarento.nsdc.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tarento.nsdc.service.ICourseService;
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
import java.util.stream.Collectors;

@Service
public class CourseServiceImplV2 implements ICourseService {

    private static final String PROCEED_FOLDER_PATH = "/home/manas/CourseBucket/proceed";
    private static final String REJECTED_FOLDER_PATH = "/home/manas/CourseBucket/rejected";
    private static final String KAFKA_TOPIC_NAME = "course";

    @Autowired
    private Producer kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

  /*  public void processIncomingFile(String fileName) {
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
                        if (!isValidHeader || !isValidData) {
                            moveFile(file, REJECTED_FOLDER_PATH, "rejected");
                            return false;
                        }
                        Map<String, Object> dataMap = convertRowToDataMap(dataRow, headerRow);
                        dataMap.put("id", UUID.randomUUID().toString());
                        kafkaTemplate.push(KAFKA_TOPIC_NAME, dataMap);
                        System.out.println("Data sent to Kafka for sheet: " + sheet.getSheetName());
                    }
                }
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private boolean validateRow(Row row) {
        for (Cell cell : row) {
            CellType cellType = cell.getCellType();
            if (cellType == CellType.NUMERIC || cellType == CellType.FORMULA) {
                if (cell.getCellType() == CellType.FORMULA) {
                    cellType = cell.getCachedFormulaResultType();
                }

                if (cellType == CellType.NUMERIC) {
                    double numericValue = cell.getNumericCellValue();
                    if (Double.isNaN(numericValue) || Double.isInfinite(numericValue)) {
                        return false;
                    }
                }
            } else if (cellType == CellType.STRING) {
                String cellValue = cell.getStringCellValue().trim();
                if (cellValue.isEmpty()) {
                    return false;
                }
            } else if (cellType != CellType.BLANK) {
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

    /*private Map<String, String> convertRowToDataMap(Row row, Row headerRow) {
        Map<String, String> dataMap = new HashMap<>();
        DataFormatter formatter = new DataFormatter();
        for (int colIndex = 0; colIndex < headerRow.getLastCellNum(); colIndex++) {
            Cell headerCell = headerRow.getCell(colIndex);
            Cell cell = row.getCell(colIndex);
            if (headerCell != null && cell != null) {
                String propertyName = headerCell.getStringCellValue().trim();
                String cellValue = formatter.formatCellValue(cell).trim();
                dataMap.put(propertyName, cellValue);
            }
        }
        return dataMap;
    }*/

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
            File jsonFile = new File("/home/manas/POC/nsdc/headerToPropertyMapping.json"); // Replace with the actual file path
            mapping = objectMapper.readValue(jsonFile, Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mapping;
    }
}