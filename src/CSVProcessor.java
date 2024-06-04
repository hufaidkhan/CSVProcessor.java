import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class CSVProcessor {
    private String inputFolderPath;
    private String outputFolderPath;
    private int[] requiredColumns;
    private String[] columnHeaders;

    public CSVProcessor(String inputFolderPath, String outputFolderPath, int[] requiredColumns, String[] columnHeaders) {
        this.inputFolderPath = inputFolderPath;
        this.outputFolderPath = outputFolderPath;
        this.requiredColumns = requiredColumns;
        this.columnHeaders = columnHeaders;
    }

    public void processCSV() throws IOException {
        File inputFolder = new File(inputFolderPath);
        File outputFolder = new File(outputFolderPath);

        // Get the list of files in the input folder
        File[] inputFiles = inputFolder.listFiles((dir, name) -> name.endsWith(".csv"));

        if (inputFiles == null || inputFiles.length == 0) {
            System.out.println("No CSV files found in the input folder.");
            return;
        }

        for (File inputFile : inputFiles) {
            File outputFile = new File(outputFolder, "VeeqoToNet32 " + new SimpleDateFormat("ddMMMyyyy_HHmmss").format(new Date()) + ".csv");

            try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
                // Write column headers
                writer.write(String.join(",", columnHeaders));
                writer.newLine();

                Set<String> processedOrderIds = new HashSet<>();
                Set<String> processedTrackingNumbers = new HashSet<>();

                boolean firstLine = true; // Flag to skip the first line
                while (reader.ready()) {
                    String line = reader.readLine();
                    if (firstLine) {
                        firstLine = false;
                        continue; // Skip the first line
                    }
                    String[] parts = line.split(",");
                    String orderId = parts[1]; // Assuming column 1 contains the order ID
                    String trackingNumber = extractTrackingNumber(parts); // Custom method to extract tracking number

                    // Check for duplicate order IDs or tracking numbers
                    if (processedOrderIds.contains(orderId) || processedTrackingNumbers.contains(trackingNumber)) {
                        // Skip this line
                        continue;
                    }

                    processedOrderIds.add(orderId);
                    processedTrackingNumbers.add(trackingNumber);

                    List<String> extractedColumns = new ArrayList<>();

                    // Add fixed value "516454" to the new column at index 1
                    extractedColumns.add(parts[1]); // Add data from the second column (0-based index)
                    extractedColumns.add("516454"); // Add fixed value at index 1

                    SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
                    SimpleDateFormat outputDateFormat = new SimpleDateFormat("MM/dd/yyyy");
                    Date date = inputDateFormat.parse(parts[22]); // Parse the date from column 22
                    extractedColumns.add(outputDateFormat.format(date)); // Format and add the date to the output

                    String carrier = identifyCarrier(trackingNumber); // Identify carrier

                    extractedColumns.add(carrier); // Add carrier information
                    extractedColumns.add(trackingNumber); // Add tracking number

                    writer.write(String.join(",", extractedColumns));
                    writer.newLine();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            // Delete the input file
            if (!inputFile.delete()) {
                System.out.println("Failed to delete input file: " + inputFile.getName());
            }
        }
    }

    private String extractTrackingNumber(String[] parts) {
        for (String part : parts) {
            if (part.startsWith("91") || part.startsWith("92") || part.startsWith("93")) {
                // Check if the part matches the USPS tracking number format
                if (part.length() == 22) {
                    return part;
                }
            }
        }
        return "";
    }

    private String identifyCarrier(String trackingNumber) {
        if (trackingNumber.startsWith("1Z") && trackingNumber.length() == 18) {
            return "UPS";
        } else if (trackingNumber.matches("91\\d{18,22}|92\\d{18,22}|93\\d{18,22}")) {
            return "USPS";
        } else {
            return ""; // Empty for other carriers
        }
    }

    public static void main(String[] args) {
        // Input and output folder paths
        String inputFolderPath = System.getProperty("user.dir"); // Use the current directory as input folder
        String outputFolderPath = System.getProperty("user.home") + "\\Downloads"; // Use the Downloads folder as output folder

        // Define required columns (0-based index)
        int[] requiredColumns = {1, 22}; // Example: Extracting columns 1 and 22
        String[] columnHeaders = { "orderid", "shipdate", "carrier", "tracking" }; // Define column headers

        CSVProcessor csvProcessor = new CSVProcessor(inputFolderPath, outputFolderPath, requiredColumns, columnHeaders);

        try {
            csvProcessor.processCSV();
            System.out.println("CSV processing complete.");
        } catch (IOException e) {
            System.err.println("Error processing CSV file: " + e.getMessage());
        }
    }
}
