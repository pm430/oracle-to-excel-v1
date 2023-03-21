package org.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OracleToExcel {

    private static final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final String[][] COLUMN_NAMES_AND_QUERIES = {
            {
                    "DEPARTMENT_ID", "DEPARTMENT_NAME", "MANAGER_ID", "LOCATION_ID",
                    "SELECT department_id, department_name, manager_id, location_id FROM departments"
            },
            {
                    "EMPLOYEE_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE_NUMBER", "HIRE_DATE", "JOB_ID", "SALARY",
                    "COMMISSION_PCT", "MANAGER_ID", "DEPARTMENT_ID",
                    "SELECT employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id FROM employees"
            },
            {
                    "JOB_ID", "JOB_TITLE", "MIN_SALARY", "MAX_SALARY",
                    "SELECT job_id, job_title, min_salary, max_salary FROM jobs"
            }
    };

    public static void main(String[] args) {
        HikariDataSource dataSource = null;
        try (SXSSFWorkbook workbook = new SXSSFWorkbook()) {
            dataSource = createDataSource();
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            for (String[] columnNamesAndQuery : COLUMN_NAMES_AND_QUERIES) {
                Sheet sheet = workbook.createSheet(columnNamesAndQuery[0]);
                List<Object[]> rowDataList = getDataFromDatabase(dataSource, columnNamesAndQuery[1]);
                writeDataToSheet(sheet, rowDataList, executor);
            }
            saveWorkbookToFile(workbook);
            closeExecutorService(executor);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeDataSource(dataSource);
        }
    }

    private static HikariDataSource createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:oracle:thin:@localhost:1521:xe");
        config.setUsername("hr");
        config.setPassword("hr");
        config.setMaximumPoolSize(THREAD_POOL_SIZE);
        config.setMinimumIdle(THREAD_POOL_SIZE);
        return new HikariDataSource(config);
    }

    private static List<Object[]> getDataFromDatabase(HikariDataSource dataSource, String query) throws Exception {
        List<Object[]> rowDataList = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(query);
             ResultSet rs = pstmt.executeQuery()) {
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                Object[] rowData = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    rowData[i] = rs.getString(i + 1);
                }
                rowDataList.add(rowData);
            }
        }
        return rowDataList;
    }

    private static void writeDataToSheet(Sheet sheet, List<Object[]> rowDataList, ExecutorService executor) {
        String[] columnNames = (String[]) rowDataList.get(0);
        int rowNum = 0;
        Row headerRow = sheet.createRow(rowNum++);
        for (int i = 0; i < columnNames.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(columnNames[i]);
        }
        int dataSize = rowDataList.size();
        int partitionSize = dataSize / THREAD_POOL_SIZE + 1;
        for (int i = 0; i < THREAD_POOL_SIZE; i++) {
            int fromIndex = i * partitionSize;
            int toIndex = Math.min((i + 1) * partitionSize, dataSize);
            List<Object[]> partition = rowDataList.subList(fromIndex, toIndex);
            int finalRowNum = rowNum;
            executor.submit(() -> {
                writePartitionToSheet(sheet, partition, finalRowNum, columnNames);
            });
            rowNum += partition.size();
        }
        executor.shutdown();
        awaitExecutorService(executor);
    }

    private static void writePartitionToSheet(Sheet sheet, List<Object[]> partition, int rowNum, String[] columnNames) {
        for (Object[] rowData : partition) {
            Row row = sheet.createRow(rowNum++);
            for (int i = 0; i < columnNames.length; i++) {
                Cell cell = row.createCell(i);
                cell.setCellValue((String) rowData[i]);
            }
        }
    }

    private static void saveWorkbookToFile(SXSSFWorkbook workbook) throws Exception {
        try (FileOutputStream fos = new FileOutputStream("output.xlsx")) {
            workbook.write(fos);
        }
    }

    private static void closeExecutorService(ExecutorService executor) throws Exception {
        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }
    }

    private static void closeDataSource(HikariDataSource dataSource) {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    private static void awaitExecutorService(ExecutorService executor) {
        try {
            executor.shutdown();
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
