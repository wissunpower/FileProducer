package app;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

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
	
	static final String CONFIGNAME_SIMPLESCHEMA = "_simple_schema";
	static boolean isSimpleSchema = false;

	public static boolean isSimpleSchema() {
		return isSimpleSchema;
	}

	public static void setSimpleSchema(boolean isSimpleSchema) {
		FileProducer.isSimpleSchema = isSimpleSchema;
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
		Sheet sheet1 = wb.getSheetAt( 0 );

		Map< Integer, String > parseResult = parseExcelSheetToJson( sheet1 );
		
		for ( Integer key : parseResult.keySet() ) {
			producer.send( new ProducerRecord< String, String >( getConfigTopicName(), key.toString(), parseResult.get( key ) ) );
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
			else if ( name.equals( CONFIGNAME_SIMPLESCHEMA ) ) {
				if ( value.equals( "true" ) )
					setSimpleSchema( true );
			}
			else {
				// Kafka Producer API �⺻ ���� ó��.
				// �ϴ� ��� ������ value�� String �������� �Է��Ѵ�.
				outProp.put( name, value );
			}
			
			System.out.println( "name = " + name + ", value = " + value );
		}
		configFile.close();
	}
	
	public static Map< Integer, String > parseExcelSheetToJson( Sheet sheet ) {
		DataFormatter formatter = new DataFormatter();
		Map< Integer, String > resultMap = new TreeMap< Integer, String >();

		for ( Row row : sheet ) {
			String schemaStr = "{\"type\":\"struct\",\"fields\":[";
			String payloadStr = "{";
			for ( Cell cell : row ) {
				String typeStr = "string";
				String fieldStr = "\"" + String.valueOf( cell.getColumnIndex() ) + "\"";
				String valueStr = formatter.formatCellValue( cell );
				valueStr = valueStr.replace( "\"", "\\\"" );
				
				switch ( cell.getCellTypeEnum() ) {
				case NUMERIC:
					if ( DateUtil.isCellDateFormatted( cell ) ) {
						// cell.getDateCellValue();
						valueStr = ( "\"" + valueStr + "\"" );
					} else {
						// cell.getNumericCellValue();
						typeStr = "int32";
					}
					break;
				case BOOLEAN:
					// cell.getBooleanCellValue();
					valueStr = ( "\"" + valueStr + "\"" );
					break;
				case FORMULA:
					// cell.getCellFormula();
					valueStr = ( "\"" + valueStr + "\"" );
					break;
				case BLANK:
					valueStr = ( "\"\"" );
					break;
				default:	// case STRING:
					// cell.getRichStringCellValue().getString();
					valueStr = ( "\"" + valueStr + "\"" );
					break;
				}
				
				schemaStr += ( "{\"type\":\"" + typeStr + "\",\"optional\":false,\"field\":" + fieldStr + "}" );
				payloadStr += ( fieldStr + ":" + valueStr );
				if ( cell.getColumnIndex() != ( row.getLastCellNum() - 1 ) ) {
					schemaStr += ",";
					payloadStr += ",";
				}
			}
			
			schemaStr += ( "],\"optional\":false,\"name\":\"sheetrow\"}" );
			payloadStr += "}";
			
			schemaStr = schemaStr.replace( "\r", "" );
			schemaStr = schemaStr.replace( "\n", "" );
			payloadStr = payloadStr.replace( "\r", "" );
			payloadStr = payloadStr.replace( "\n", "" );
			
			String result = payloadStr;
			if ( !isSimpleSchema() ) {
				result = "{\"schema\":" + schemaStr + ",\"payload\":" + payloadStr + "}";
			}
			
			System.out.println( "Key : " + String.valueOf( row.getRowNum() ) + ", Value : " + result );
			
			resultMap.put( row.getRowNum(), result );
		}
		
		return resultMap;
	}
}
