package org.librairy.service.space.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.librairy.service.space.facade.model.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class CSVWriter {

    private static final Logger LOG = LoggerFactory.getLogger(CSVWriter.class);

    private String outputDir = "output";

    private ObjectMapper jsonMapper = new ObjectMapper();

    private BufferedWriter writer;

    public void open() throws IOException {
        Path filePath      = Paths.get(outputDir, "vectors.csv.gz");

        filePath.toFile().getParentFile().mkdirs();

        writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(filePath.toFile()))));
    }

    public void close() throws IOException {
        writer.flush();
        writer.close();
    }

    public void write(DirichletDistribution point){

        String separator = ";;";
        try {

            writer.write(jsonMapper.writeValueAsString(point));
            writer.write("\n");


        } catch (IOException e) {
            LOG.error("Error writing on file: " + point, e);
        }


    }
}
