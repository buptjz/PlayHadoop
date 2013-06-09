/**很简单的JAVACV模糊图片的例子**/

import static com.googlecode.javacv.cpp.opencv_core.*;
import static com.googlecode.javacv.cpp.opencv_imgproc.*;
import static com.googlecode.javacv.cpp.opencv_highgui.*;

public class JavaCVDemo {
	public static void main(String[] args) throws Exception {
		//smooth("/home/wangjz/Desktop/localImage/2.jpg");
		IplImage image = cvLoadImage("/home/wangjz/Desktop/localImage/2.jpg");
		IplImage hsvImage = rgb2Hsv(image);
		System.out.println(hsvImage); 
		
	}
	public static void smooth(String filename) { 
		IplImage image = cvLoadImage(filename);

		if (image != null) {
			cvSmooth(image, image, CV_GAUSSIAN, 3);
			cvSaveImage("/home/wangjz/Desktop/localImage/22.jpg", image);
			cvReleaseImage(image);
		}
	}
	public static IplImage rgb2Hsv(IplImage bgr){
		/* 
        Converts a BGR image to HSV colorspace 
        @param bgr image to be converted 
        @return Returns bgr converted to a 3-channel, 32-bit HSV image with 
          S and V values in the range [0,1] and H value in the range [0,360] 
		 */  
		IplImage bgr32f, hsv;  
		bgr32f = cvCreateImage( cvGetSize(bgr),IPL_DEPTH_32F, 3 );
		hsv = cvCreateImage( cvGetSize(bgr), IPL_DEPTH_32F, 3 );  
		cvConvertScale(bgr,bgr32f, 1.0 /255.0, 0 );//使用线性变换转换数组输入数组.dst输出数组scale比例因子.shift该加数被加到输入数组元素按比例缩放后得到的元素上 
		//cvAvg(bgr32f);
		cvCvtColor( bgr32f, hsv, CV_BGR2HSV );//cvCvtColor(),是Opencv里的颜色空间转换函数，可以实现RGB颜色向HSV,HSI等颜色空间的转换，也可以转换为灰度图像。
		cvReleaseImage(bgr32f );
		return hsv;  

	}

}