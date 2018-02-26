package org.librairy.service.space.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.util.Strings;
import org.librairy.service.space.facade.model.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class CSVReader {

    private static final Logger LOG = LoggerFactory.getLogger(CSVReader.class);

    private String outputDir = "output";

    private ObjectMapper jsonMapper = new ObjectMapper();

    private BufferedReader reader;

    public void open() throws IOException {
        Path filePath      = Paths.get(outputDir, "vectors.csv.gz");

        filePath.toFile().getParentFile().mkdirs();

        reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(filePath.toFile()))));
    }

    public void close() throws IOException {

        reader.close();
    }

    public DirichletDistribution read() throws IOException {
        String string = reader.readLine();
        if (Strings.isNullOrEmpty(string)) return new DirichletDistribution("",Collections.emptyList());
        return jsonMapper.readValue(string,DirichletDistribution.class);


    }
}
