package=$1

rm -rf python
rm -rf lambda_layer
mkdir python
pip install $package -t python/ # insert any pip available module, repeat if necessary
zip -r lambda_layer.zip ./python
#aws s3 cp lambda_layer.zip s3://config-lambda/layers/$package/
#aws lambda publish-layer-version \
#    --layer-name $package \
#    --content S3Bucket=config-lambda,S3Key=layers/$package/lambda_layer.zip \
#    --compatible-runtimes python3.7
#rm -r python 
#rm lambda_layer.zip
