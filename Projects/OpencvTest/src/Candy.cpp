#include "cv.h"
#include "cxcore.h"
#include "highgui.h"

int main( int argc, char** argv )
{
	char *iamgePath = argv[1];
	char *savePath = argv[2];

	IplImage* pImg = NULL;
	IplImage* pCannyImg = NULL;

	//载入图像，强制转化为Gray
	pImg = cvLoadImage( iamgePath, 0);///home/wangjz/Desktop/1.jpg
	//为canny边缘图像申请空间
	pCannyImg = cvCreateImage(cvGetSize(pImg), IPL_DEPTH_8U, 1);
	//canny边缘检测
	cvCanny(pImg, pCannyImg, 50, 150, 3);
	//		//创建窗口
	//		cvNamedWindow("src", 1);
	//		cvNamedWindow("canny",1);
	//		//显示图像
	//		cvShowImage( "src", pImg );
	//		cvShowImage( "canny", pCannyImg );
	//		//等待按键
	//		cvWaitKey(0);
	//		//销毁窗口
	//		cvDestroyWindow( "src" );
	//		cvDestroyWindow( "canny" );

	cvSaveImage(savePath,pCannyImg);

	//释放图像
	cvReleaseImage( &pImg );
	cvReleaseImage( &pCannyImg );

	return 0;
}//wangjz@wangjz-laptop:~/workspace/OpencvTest/Debug$ ./OpencvTest /home/wangjz/Desktop/1.jpg
//~/workspace/OpencvTest/Debug$ ./OpencvTest /home/wangjz/Desktop/1.jpg /home/wangjz/Desktop/1_.jpg
