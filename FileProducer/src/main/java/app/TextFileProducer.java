package app;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

public class TextFileProducer {
	
	static final int CONFIG_FILENAME_ARGINDEX = 0;
	static final String CONFIG_FILENAME_DEFAULT = "config.txt";
	
	static final String CONFIGNAME_TOPICNAME = "_topic_name";
	static final String DEFAULTCONFIG_TOPICNAME = "my-topic";
	static String configTopicName = DEFAULTCONFIG_TOPICNAME;

	public static String getConfigTopicName() {
		return configTopicName;
	}

	public static void setConfigTopicName(String configTopicName) {
		TextFileProducer.configTopicName = configTopicName;
	}

	static final String CONFIGNAME_INPUTFILENAME = "_input_file_name";
	static String configInputFileName = "";

	public static String getConfigInputFileName() {
		return configInputFileName;
	}

	public static void setConfigInputFileName(String configInputFileName) {
		TextFileProducer.configInputFileName = configInputFileName;
	}
	
	static final String CONFIGNAME_SIMPLESCHEMA = "_simple_schema";
	static boolean isSimpleSchema = false;

	public static boolean isSimpleSchema() {
		return isSimpleSchema;
	}

	public static void setSimpleSchema(boolean isSimpleSchema) {
		TextFileProducer.isSimpleSchema = isSimpleSchema;
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

		// text file read
		InputStream inp = new FileInputStream( getConfigInputFileName() );
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader( new InputStreamReader( inp ) );
		
		String line;
		int i = 0;
		while ( ( line = br.readLine() ) != null )
		{
			producer.send( new ProducerRecord< String, String >( getConfigTopicName(), Integer.toString( i ), line ) );
			++i;
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
}
