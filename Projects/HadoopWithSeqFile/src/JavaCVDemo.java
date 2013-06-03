/**很简单的JAVACV模糊图片的例子**/

import static com.googlecode.javacv.cpp.opencv_core.*;
import static com.googlecode.javacv.cpp.opencv_imgproc.*;
import static com.googlecode.javacv.cpp.opencv_highgui.*;

public class JavaCVDemo {
	public static void main(String[] args) throws Exception {
		smooth("/home/wangjz/Desktop/localImage/2.jpg");
	}
    public static void smooth(String filename) { 
        IplImage image = cvLoadImage(filename);

        if (image != null) {
            cvSmooth(image, image, CV_GAUSSIAN, 3);
            cvSaveImage("/home/wangjz/Desktop/localImage/22.jpg", image);
            cvReleaseImage(image);
        }
    }
}