package one.utilix;

import java.io.*;
import java.sql.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.*;

public class App {

    private static final Logger logger = Logger.getLogger(App.class.getName());

    public static void main(String[] args) {
        setupLogger();

        ensureDirectoriesExist("Test/Data", "Test/Archive", "Test/Processed");

        String folderPath = "Test/Data";
        File folder = new File(folderPath);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".csv"));

        if (files == null || files.length == 0) {
            logger.info("No CSV files found in the Data folder.");
            return;
        }

        String dbUrl = "jdbc:mysql://localhost:3306/mbus?autoReconnect=true&useSSL=false&serverTimezone=UTC";
        String dbUser = "mbus";
        String dbPassword = "mbus";

        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            logger.info("Connected to the database.");

            for (File file : files) {
                logger.info("Processing file: " + file.getName());
                try {
                    List<String[]> processedData = processCsvFile(file, connection);
                    File archiveFile = new File("Test/Archive/" + file.getName());
                    moveFile(file, archiveFile);
                    File processedFile = new File("Test/Processed/" + file.getName());
                    writeToCsv(processedData, processedFile);
                } catch (Exception e) {
                    logger.severe("Error processing file " + file.getName() + ": " + e.getMessage());
                }
            }

        } catch (SQLException e) {
            logger.severe("Database connection error: " + e.getMessage());
        }
    }

    private static void ensureDirectoriesExist(String... directories) {
        for (String dir : directories) {
            File folder = new File(dir);
            if (!folder.exists()) {
                if (folder.mkdirs()) {
                    logger.info("Created directory: " + dir);
                } else {
                    logger.severe("Failed to create directory: " + dir);
                }
            } else {
                logger.info("Directory exists: " + dir);
            }
        }
    }

    // Зчитування CSV з додатковими перевірками
    private static List<String[]> processCsvFile(File file, Connection connection) throws IOException {
        List<String[]> processedData = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length < 3) {
                    logger.warning("Skipping malformed line in file " + file.getName() + ": " + line);
                    continue;
                }

                String nzav = values[0];
                float pokaz;
                long unixtime;

                // Перевірка формату показника
                try {
                    pokaz = Float.parseFloat(values[1]);
                } catch (NumberFormatException e) {
                    logger.warning("Invalid pokaz value in file " + file.getName() + ": " + values[1]);
                    continue;
                }

                // Перевірка формату часу
                try {
                    unixtime = Long.parseLong(values[2]);
                } catch (NumberFormatException e) {
                    logger.warning("Invalid timestamp in file " + file.getName() + ": " + values[2]);
                    continue;
                }

                // Перевірка наявності kvk_id
                Integer kvkId = getKvkId(connection, nzav);
                if (kvkId == null) {
                    logger.warning("No kvk_id found for nzav: " + nzav);
                    continue;
                }

                // Перевірка аномалій (закоментовано для використання на етапі відладки)
                /*
                if (isTimestampAnomalous(connection, kvkId, unixtime)) {
                    logger.warning("Anomalous timestamp for kvk_id: " + kvkId);
                }

                if (isPokazAnomalous(connection, kvkId, pokaz)) {
                    logger.warning("Anomalous pokaz value for kvk_id: " + kvkId);
                }
                */

                saveToDatabase(connection, kvkId, pokaz, unixtime, nzav);
                processedData.add(new String[]{nzav, String.valueOf(pokaz), String.valueOf(unixtime)});
            }
        } catch (IOException e) {
            logger.severe("Error reading file " + file.getName() + ": " + e.getMessage());
            throw e;
        }

        return processedData;
    }

    // Метод для отримання kvk_id за nzav
    private static Integer getKvkId(Connection connection, String nzav) {
        String query = "SELECT kvk_id FROM network WHERE meter_number = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, nzav);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("kvk_id");
            } else {
                return null;
            }
        } catch (SQLException e) {
            logger.severe("Error querying kvk_id: " + e.getMessage());
            return null;
        }
    }

    // Закоментовані методи для перевірки аномалій
    /*
    private static boolean isTimestampAnomalous(Connection connection, int kvkId, long timestamp) {
        String query = "SELECT MAX(unixtime) FROM meters_values WHERE kvk_id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, kvkId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                long maxTimestamp = rs.getLong(1);
                return timestamp <= maxTimestamp;
            }
        } catch (SQLException e) {
            logger.warning("Error checking timestamp anomaly: " + e.getMessage());
        }
        return false;
    }

    private static boolean isPokazAnomalous(Connection connection, int kvkId, float pokaz) {
        String query = "SELECT MAX(pokaz) FROM meters_values WHERE kvk_id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, kvkId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                float maxPokaz = rs.getFloat(1);
                return pokaz < maxPokaz;
            }
        } catch (SQLException e) {
            logger.warning("Error checking pokaz anomaly: " + e.getMessage());
        }
        return false;
    }
    */

    private static void saveToDatabase(Connection connection, int kvkId, float pokaz, long unixtime, String nzav) {
        String query = "INSERT INTO meters_values (kvk_id, unixtime, pokaz, nzav, sent) VALUES (?, ?, ?, ?, 0)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, kvkId);
            preparedStatement.setLong(2, unixtime);
            preparedStatement.setFloat(3, pokaz);
            preparedStatement.setString(4, nzav);
            preparedStatement.executeUpdate();
            logger.info("Data saved for kvk_id: " + kvkId);
        } catch (SQLException e) {
            logger.severe("Error inserting data into database: " + e.getMessage());
        }
    }

    private static void moveFile(File source, File destination) {
        if (source.renameTo(destination)) {
            logger.info("File moved to: " + destination.getAbsolutePath());
        } else {
            logger.severe("Failed to move file: " + source.getName());
        }
    }

    private static void writeToCsv(List<String[]> data, File file) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            for (String[] row : data) {
                bw.write(String.join(",", row));
                bw.newLine();
            }
            logger.info("Processed data saved to: " + file.getAbsolutePath());
        } catch (IOException e) {
            logger.severe("Error writing to file " + file.getName() + ": " + e.getMessage());
        }
    }

    private static void setupLogger() {
        try {
            FileHandler fileHandler = new FileHandler("app.log", true);
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);
            logger.setLevel(Level.ALL);
            logger.info("Logger initialized.");
        } catch (IOException e) {
            System.err.println("Failed to initialize logger: " + e.getMessage());
        }
    }
}
