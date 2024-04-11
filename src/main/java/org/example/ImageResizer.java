package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.imgscalr.Scalr;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

public class ImageResizer implements RequestHandler<S3Event, String> {

    private String result = "";

    public String handleRequest(S3Event event, Context context){
        event.getRecords().forEach(eventConsumer);

        return result;
    }

    private final Consumer<S3EventNotification.S3EventNotificationRecord> eventConsumer = record -> {
        String bucket = record.getS3().getBucket().getName();
        String key = record.getS3().getObject().getKey().replace('+', ' ');

        if(!key.startsWith("input/")){
            result += "Skip: " + key + "\n\n";
            return;
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        try{
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));
            BufferedImage scaledImg = scaleImage(s3Object);

            ByteArrayOutputStream os = new ByteArrayOutputStream();
            ImageIO.write(scaledImg, "jpg", os);
            byte[] buffer = os.toByteArray();
            InputStream is = new ByteArrayInputStream(buffer);

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(buffer.length);
            metadata.setContentType("image/jpeg");

            // BUGG: sparas som "resized/input/..."
            String outputKey = key.replace("input/", "");
            s3Client.putObject(bucket, "resized/" + outputKey, is, metadata);

            result += "Resized: " + key + "\n\n";

        }catch (Exception e){
            result += e + "\n\n";
        }

    };

    private static BufferedImage scaleImage(S3Object object) throws IOException {
        var img = ImageIO.read(object.getObjectContent());
        return Scalr.resize(img, 720);
    }

}
