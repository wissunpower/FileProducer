package app;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
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
	
	static final int CONFIG_FILENAME_ARGINDEX = 0;
	static final String CONFIG_FILENAME_DEFAULT = "config.txt";
	
	static final String CONFIGNAME_TOPICNAME = "_topic_name";
	static final String DEFAULTCONFIG_TOPICNAME = "my-topic";
	static String configTopicName = DEFAULTCONFIG_TOPICNAME;

	public static String getConfigTopicName() {
		return configTopicName;
	}

	public static void setConfigTopicName(String configTopicName) {
		FileProducer.configTopicName = configTopicName;
	}

	static final String CONFIGNAME_INPUTFILENAME = "_input_file_name";
	static String configInputFileName = "";

	public static String getConfigInputFileName() {
		return configInputFileName;
	}

	public static void setConfigInputFileName(String configInputFileName) {
		FileProducer.configInputFileName = configInputFileName;
	}

	public static void main(String[] args) throws EncryptedDocumentException, InvalidFormatException, IOException {
		
		// producer default properties
		Properties props = new Properties();
		props.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092" );
		props.put( ProducerConfig.ACKS_CONFIG, "all" );
		props.put( ProducerConfig.RETRIES_CONFIG, 0 );
		props.put( ProducerConfig.BATCH_SIZE_CONFIG, 16384 );
		props.put( ProducerConfig.LINGER_MS_CONFIG, 1 );
		props.put( ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432 );
		props.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer" );
		props.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer" );
		
		String configFileName = CONFIG_FILENAME_DEFAULT;
		if ( args.length > CONFIG_FILENAME_ARGINDEX ) {
			configFileName = args[ CONFIG_FILENAME_ARGINDEX ].toString();
		}
		try {
			loadConfiguration( configFileName, props );
		}
		catch ( FileNotFoundException e ) {
			System.out.println( "Not Found Configuration File. ( file name : " + configFileName + " )" );
		}
		catch ( IOException e ) {
			System.out.println( "Invalid Configuration File Format. ( file name : " + configFileName + " )" );
		}
		
		// create producer
		Producer< String, String > producer = new KafkaProducer< String, String >( props );

		// excel file read
		InputStream inp = new FileInputStream( getConfigInputFileName() );
		
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
				
				producer.send( new ProducerRecord< String, String >( getConfigTopicName(), cellRef.formatAsString(), cellRef.formatAsString() + " - " + text ) );
			}
		}

		producer.close();
	}

	public static void loadConfiguration( String fileName, Properties outProp ) throws IOException {
		BufferedReader configFile = new BufferedReader( new FileReader( fileName ) );
		String currentStr = null;
		while ( ( currentStr = configFile.readLine() ) != null ) {
			// empty line & comment check.
			if ( currentStr.length() < 2 || currentStr.substring( 0, 2 ).equals( "//" ) )
				continue;

			final String name = currentStr.substring( 0, currentStr.indexOf( '=' ) ).trim();
			final String value = currentStr.substring( currentStr.indexOf( '=' ) + 1 ).trim();
			
			if ( name.equals( CONFIGNAME_TOPICNAME ) ) {
				setConfigTopicName( value );
			}
			else if ( name.equals( CONFIGNAME_INPUTFILENAME ) ) {
				setConfigInputFileName( value );
			}
			else {
				// Kafka Producer API 기본 설정 처리.
				// 일단 모든 설정의 value를 String 형식으로 입력한다.
				outProp.put( name, value );
			}
			
			System.out.println( "name = " + name + ", value = " + value );
		}
		configFile.close();
	}
}
