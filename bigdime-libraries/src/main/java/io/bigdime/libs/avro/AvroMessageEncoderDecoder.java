/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

/**
 * This utility class can be used to encode, decode an avro message.
 * 
 * @author Neeraj Jain
 *
 */
public class AvroMessageEncoderDecoder {
	private static final Logger logger = LoggerFactory.getLogger(AvroMessageEncoderDecoder.class);
	private String schemaFileName;
	private Schema schema;
	private static final Integer DEFAULT_BUFFER_SIZE = 16384;
	private Integer bufferSize = DEFAULT_BUFFER_SIZE;

	/**
	 * Construct AvroMessageEncoderDecoder with schemaFileName and default
	 * buffer size=16384.
	 * 
	 * @param schemaFileName
	 *            name of the file containing avro schema. This file must be
	 *            present in the classpath
	 * @throws IllegalArgumentException
	 *             if the schemaFileName is null or file doesnt exist or
	 *             contains an invalid schema
	 */
	public AvroMessageEncoderDecoder(String schemaFileName) {
		this(schemaFileName, DEFAULT_BUFFER_SIZE);
	}

	/**
	 * Construct AvroMessageEncoderDecoder with schemaFileName and specified
	 * buffer size.
	 * 
	 * @param schemaFileName
	 *            name of the file containing avro schema. This file must be
	 *            present in the classpath
	 * @param bufferSize
	 *            buffer size used by {@link DecoderFactory}. If the bufferSize
	 *            is a non-positive number, default value of buffer size is used
	 * @throws IllegalArgumentException
	 *             if the schemaFileName is null or file doesnt exist or
	 *             contains an invalid schema
	 */
	public AvroMessageEncoderDecoder(String schemaFileName, int bufferSize) {
		try {
			if (schemaFileName == null)
				throw new IllegalArgumentException(
						"schemaFileName not set, set schemaFileName before calling loadSchema");
			this.schemaFileName = schemaFileName;
			if (bufferSize > 0)
				this.bufferSize = bufferSize;
			readSchema();
		} catch (final Exception ex) {
			throw new IllegalArgumentException("unable to read schema from file:" + schemaFileName, ex);
		}
	}

	private void readSchema() throws Exception {
		logger.debug("building AvroMessageEncoderDecoder, schemaFileName={}", schemaFileName);
		try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(schemaFileName)) {
			if (is == null) {
				throw new FileNotFoundException(schemaFileName);
			}
			schema = new Schema.Parser().parse(is);
		} catch (IOException ex) {
			logger.error("building AvroMessageEncoderDecoder, exception={}", ex.toString());
			throw ex;
		}
	}

	/**
	 * Encode the json document to an avro document.
	 * 
	 * @param data
	 *            {@link JsonNode} object representing input json document
	 * @return byte[] representing avro document
	 * @throws IOException
	 *             if there was any problem in reading or writing the data
	 */
	public byte[] encode(JsonNode data) throws IOException {
		InputStream input = new ByteArrayInputStream(data.toString().getBytes(Charset.defaultCharset()));
		DataInputStream in = new DataInputStream(input);

		Decoder decoder = DecoderFactory.get().jsonDecoder(schema, in);

		DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
		Object datum = reader.read(null, decoder);

		GenericDatumWriter<Object> w = new GenericDatumWriter<Object>(schema);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

		Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);

		w.write(datum, e);
		e.flush();

		return outputStream.toByteArray();
	}

	/**
	 * Converts the avro document to {@link JsonNode} object.
	 * 
	 * @param data
	 *            input avro document
	 * @return {@link JsonNode} object representing output Json document
	 * @throws IOException
	 *             if there was any problem in reading or writing the data
	 */
	public JsonNode decode(byte[] data) throws IOException {
		InputStream input = new ByteArrayInputStream(data);
		DataInputStream in = new DataInputStream(input);
		logger.debug("Decoding message: {} ", new String(data, Charset.defaultCharset()));

		DecoderFactory df = new DecoderFactory();
		df.configureDecoderBufferSize(bufferSize.intValue());
		Decoder decoder = df.binaryDecoder(in, null);
		logger.debug("decoding", decoder.toString());

		DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
		Object datum = reader.read(null, decoder);
		JsonNode jn = new ObjectMapper().readTree(datum.toString());
		return jn;
	}

}
