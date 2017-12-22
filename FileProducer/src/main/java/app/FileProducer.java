package app;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;

public class FileProducer {

	public static void main(String[] args) throws EncryptedDocumentException, InvalidFormatException, IOException {
		
		// create producer
		Properties props = new Properties();
		props.put( "bootstrap.servers", "localhost:9092" );
		props.put( "acks", "all" );
		props.put( "retries", 0 );
		props.put( "batch.size", 16384 );
		props.put( "linger.ms", 1 );
		props.put( "buffer.memory", 33554432 );
		props.put( "key.serializer", "org.apache.kafka.common.serialization.StringSerializer" );
		props.put( "value.serializer", "org.apache.kafka.common.serialization.StringSerializer" );
		
		Producer< String, String > producer = new KafkaProducer< String, String >( props );
		String topicName = "my-topic";

		// excel file read
		InputStream inp = new FileInputStream( "open_dataset_list.xlsx" );
		
		Workbook wb = WorkbookFactory.create( inp );

		DataFormatter formatter = new DataFormatter();
		Sheet sheet1 = wb.getSheetAt( 0 );
		
		for ( Row row : sheet1 ) {
			for ( Cell cell : row ) {
				CellReference cellRef = new CellReference( row.getRowNum(), cell.getColumnIndex() );
				System.out.println( cellRef.formatAsString() );
				System.out.println( " - " );
				
				String text = formatter.formatCellValue( cell );
				System.out.println( text );
				
				switch ( cell.getCellTypeEnum() ) {
				case STRING:
					System.out.println( cell.getRichStringCellValue().getString() );
					break;
				case NUMERIC:
					if ( DateUtil.isCellDateFormatted( cell ) ) {
						System.out.println( cell.getDateCellValue() );
					} else {
						System.out.println( cell.getNumericCellValue() );
					}
					break;
				case BOOLEAN:
					System.out.println( cell.getBooleanCellValue() );
					break;
				case FORMULA:
					System.out.println( cell.getCellFormula() );
					break;
				case BLANK:
					System.out.println();
					break;
				default:
					System.out.println();
				}
				
				producer.send( new ProducerRecord< String, String >( topicName, cellRef.formatAsString(), text ) );
			}
		}

		producer.close();
	}

}
