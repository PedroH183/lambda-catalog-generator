import {S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";

const client = new S3Client({region: ""}); // Coloque sua região da AWS aqui !

export const handler = async (event) => {
  try {
    for(const record of event.Records) {
      console.log("Iniciando processamento de mensagem", record)
      
      const rawBody = JSON.parse(record.body);
      
      try {
        var bucketName = "" // Mude para o nome do seu Bucket
        var filename = `${rawBody.ownerId}-catalog.json` // Altere o nome 
        const catalog = await getS3Object(bucketName, filename);
        const catalogData = JSON.parse(catalog)
      
        if(rawBody.type == "product") {
          updateOrAddItem(catalogData.products, rawBody)
        } else {
          updateOrAddItem(catalogData.categories, rawBody)
        }
        
        await putS3Object(bucketName, filename, JSON.stringify(catalogData));
      
      } catch (error) {
        if(error.message == "Error getting object from bucket") {
          const newCatalog = { products: [], categories: [] }
          if(rawBody.type == "product") {
            newCatalog.products.push(rawBody);
          } else {
            newCatalog.categories.push(rawBody);
          }
          
          await putS3Object(bucketName, filename, JSON.stringify(newCatalog))
        }
        else {
          throw error;
        }
      }
  }
    
    return { status: 'sucesso' }
  } catch (error) {
    console.log("Error", error)
    throw new Error("Erro ao processar mensagem do SQS");
  }
};


async function getS3Object(bucket, key) {
    const getCommand = new GetObjectCommand({
      Bucket: bucket,
      Key: key
    });

    try {
      const response = await client.send(getCommand);
      return streamToString(response.Body);

    } catch (error) {
      throw new Error('Error getting object from bucket');
    }
}

function updateOrAddItem(catalog, newItem){
  const index = catalog.findIndex(item => item.id === newItem.id)
  if(index !== -1){
    catalog[index] = {...catalog[index], ...newItem}
  } else {
    catalog.push(newItem)
  }
}

async function putS3Object(dstBucket, dstKey, content) {
    try {
      const putCommand = new PutObjectCommand({
        Bucket: dstBucket,
        Key: dstKey,
        Body: content,
        ContentType: "application/json"
      });
    
      const putResult = await client.send(putCommand);
  
      return putResult;
    
    } catch (error) {
      console.log(error);
      return;
    }
}


function streamToString(stream) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      stream.on('data', (chunk) => chunks.push(chunk));
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
      stream.on('error', reject);
    });
}
