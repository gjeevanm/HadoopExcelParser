package process;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Iterator;

/*
Reads a input excel file using POI , parses the files and loads into hadoop
 */
public class StartProcess {

    public static void main(String[] args) throws IOException {


        final String FILE_NAME = args[0];
        final char ctrlA = '\u0001';
        FSDataOutputStream fout = null;
        Sheet datatypeSheet = null;
        String valueHolder = "";

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost");
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName());

        conf.addResource(new Path(
                args[2]));
        conf.addResource(new Path(
                args[3]));
        conf.addResource(args[4]);
        System.setProperty("hadoop.home.dir", "/");
        System.setProperty("HADOOP_USER_NAME", args[5]);
        FileSystem fs = FileSystem.get(conf);

        try {


            FSDataInputStream excelFile = fs.open(new Path(FILE_NAME));
            Workbook workbook = new XSSFWorkbook(excelFile);
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                datatypeSheet = workbook.getSheetAt(i);
                fs.delete(new Path(args[1] + "_" + i));
                fout = fs.create(new Path(args[1] + "_" + i));


                Iterator<Row> iterator = datatypeSheet.iterator();

                while (iterator.hasNext()) {

                    Row currentRow = iterator.next();

                    for (int k = 0; k < currentRow.getPhysicalNumberOfCells(); k++) {
                        Cell currentCell = currentRow.getCell(k, currentRow.CREATE_NULL_AS_BLANK);

                        if (currentCell.getCellTypeEnum() == CellType.STRING) {
                            valueHolder = valueHolder.concat(currentCell.getStringCellValue() + ctrlA);


                        } else if (currentCell.getCellTypeEnum() == CellType.NUMERIC) {

                            if (HSSFDateUtil.isCellDateFormatted(currentCell)) {

                                DateFormat df = new SimpleDateFormat("yyyy/MM/dd");

                                valueHolder = valueHolder.concat(df.format(currentCell.getDateCellValue()) + ctrlA);

                            } else {
                                String s = String.valueOf(currentCell.getNumericCellValue());

                                valueHolder = valueHolder.concat(s + ctrlA);

                            }
                        } else if (currentCell.getCellTypeEnum() == CellType.BLANK || currentCell == null) {

                            valueHolder = valueHolder.concat("");

                        }

                    }

                    valueHolder = valueHolder.substring(0, valueHolder.length() - 1);

                    fout.writeBytes(valueHolder + "\n");
                    System.out.println(valueHolder);
                    valueHolder = "";
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fout.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
