import * as minio from "minio";
import { strings } from "../../common/strings";
import { AppError,  AssetLabelingState,  AssetState,  AssetType,  ErrorCode, IAsset, ILabelData, StorageType } from '../../models/applicationState';
import { IStorageProvider } from "./storageProviderFactory";
import { throwUnhandledRejectionForEdge } from "../../react/components/common/errorHandler/errorHandler";
import { withQueueMap } from "../../common/queueMap/withQueueMap";
import { AssetService } from "../../services/assetService";
import { constants } from "../../common/constants";


/**
 * Options for Minio Cloud Storage
 * @member endPoint - 
 * @member port - 
 * @member useSSL
 * @member accessKey
 * @member secretKey
 */
 export interface IMinioCloudStorageOptions {
    endPoint: string;
    port: number;
    useSSL: boolean,
    accessKey: string;
    secretKey: string;
}

/**
 * Storage Provider for Minio Storage
 */
 @withQueueMap
export class MinioStorage implements IStorageProvider {
    /**
     * Storage type
     * @returns - StorageType.Cloud
     */
    public storageType: StorageType = StorageType.Cloud;

    private minioClient : minio.Client;
    constructor(private options?:IMinioCloudStorageOptions){
        if(this.options['useSSL']===true){
            this.options = {
                endPoint: options!.endPoint!,
                port: options!.port!,
                useSSL: false,
                accessKey: options!.accessKey!,
                secretKey: options.secretKey!
            }
            this.minioClient = new minio.Client(this.options)
        }
    }

    public async initialize(): Promise<void> { }

    /**
     * Reads text from specified minio
     * @param objectName - Object of minio in bucket
     */

    public async readText(objectName: string, ignoreNotFound?: boolean | undefined): Promise<string> {
        try {
            const data = await this.getObjectContent(objectName)
            return data
        } catch (exception) {
            this.minioStorageErrorHandler(exception)
        }   
    }

    public async isValidProjectConnection(filepath?: any): Promise<boolean> {
        try {
            const client = new minio.Client(this.options);
            if(client){
                return true
            }             
            else{
                return false
            }      
        } catch {
            return false;
        }
    }
    public async readBinary(objectName: string): Promise<Buffer> {
        try {
            const data = await this.getObjectContent(objectName)
            var arrayBuffer = new ArrayBuffer(data.length*2)
            var bufView = new Uint16Array(arrayBuffer)
            for(var i=0; i < data.length; i++){
                bufView[i] = data.charCodeAt(i)
            }
            return Buffer.from(arrayBuffer)
        } catch (exception) {
            this.minioStorageErrorHandler(exception)
        }   
    }
    public async deleteFile(objectName: string, ignoreNotFound?: boolean, ignoreForbidden?: boolean): Promise<void> {
        try {
            const bucketName = await this.getMinioBucket()
            await this.minioClient.removeObject(bucketName, objectName)
        } catch (exception) {
            this.minioStorageErrorHandler(exception, ignoreNotFound, ignoreForbidden);  
        }
    }
    public async writeText(objectName: string, content: string | Buffer, folderPath?: string): Promise<void> {
        try {
            const bucketName = await this.getMinioBucket()
            await this.minioClient.putObject(bucketName, objectName, content);
        } catch (exception) {
            this.minioStorageErrorHandler(exception);
        }
    }
    public async writeBinary(objectName: string, content: Buffer): Promise<void> {
        try {
            const bucketName = await this.getMinioBucket()
            await this.minioClient.putObject(bucketName, objectName, content);
        } catch (exception) {
            this.minioStorageErrorHandler(exception);
        }
    }
    public async listFiles(objectPath?: string, ext?: string): Promise<string[]> {
        try {         
            const bucketName = await this.getMinioBucket()
            const result = await this.getFileList(bucketName, objectPath)
            return result
        } catch (exception) {
            this.minioStorageErrorHandler(exception);
        }
    }
    public async isFileExists(filepath: string, folderPath?: string): Promise<boolean> {
        const bucketName = await this.getMinioBucket()

        const isFileExist = await this.minioClient.getObject(bucketName, filepath).then((dataStream)=>{
            return true
        }).catch((err)=>{
            if(err.code==='NoSuchKey'){
                return false
            }
        })

        return isFileExist
    }
    listContainers(folderPath?: string): Promise<string[]> {
        throw new Error('Method not implemented.');
    }
    createContainer(folderPath: string): Promise<void> {
        throw new Error('Method not implemented.');
    }
    deleteContainer(folderPath: string): Promise<void> {
        throw new Error('Method not implemented.');
    }
    public async getAssets(folderPath?: string, folderName?: string): Promise<IAsset[]> {
        const files: string[] = await this.listFiles(folderPath);
        const result: IAsset[] = [];
        await Promise.all(files.map(async (file) => {
            const url = await this.getUrl(file);
            const asset = await AssetService.createAssetFromFilePath(url, file);
            if (this.isSupportedAssetType(asset.type)) {
                const labelFileName = decodeURIComponent(`${asset.name}${constants.labelFileExtension}`);
                const ocrFileName = decodeURIComponent(`${asset.name}${constants.ocrFileExtension}`);

                if (files.find((str) => str === labelFileName)) {
                    asset.state = AssetState.Tagged;
                    const labelFileName = decodeURIComponent(`${asset.name}${constants.labelFileExtension}`);
                    const json = await this.readText(labelFileName, true);
                    const labelData = JSON.parse(json) as ILabelData;
                    if (labelData) {
                        asset.labelingState = labelData.labelingState || AssetLabelingState.ManuallyLabeled;
                        asset.schema = labelData.$schema;
                    }
                } else if (files.find((str) => str === ocrFileName)) {
                    asset.state = AssetState.Visited;
                } else {
                    asset.state = AssetState.NotVisited;
                }
                result.push(asset);
            }
        }));
        return result;
    }
    public async getAsset(folderPath: string, assetName: string): Promise<IAsset> {
        const files: string[] = await this.listFiles(folderPath);
        if(files.findIndex(f=>f===assetName)!==-1){
            const url = await this.getUrl(assetName);
            const asset = await AssetService.createAssetFromFilePath(url, assetName);
            if (this.isSupportedAssetType(asset.type)) {
                const labelFileName = decodeURIComponent(`${asset.name}${constants.labelFileExtension}`);
                const ocrFileName = decodeURIComponent(`${asset.name}${constants.ocrFileExtension}`);

                if (files.find((str) => str === labelFileName)) {
                    asset.state = AssetState.Tagged;
                    const labelFileName = decodeURIComponent(`${asset.name}${constants.labelFileExtension}`);
                    const json = await this.readText(labelFileName, true);
                    const labelData = JSON.parse(json) as ILabelData;
                    if (labelData) {
                        asset.labelingState = labelData.labelingState || AssetLabelingState.ManuallyLabeled;
                        asset.schema = labelData.$schema;
                    }
                } else if (files.find((str) => str === ocrFileName)) {
                    asset.state = AssetState.Visited;
                } else {
                    asset.state = AssetState.NotVisited;
                }
                return asset;
            }
        }
        else {
            return null;
        }
    }

    private async getMinioBucket(folderPath?: string){

        const bucketName = 'datasets'

        const isExists = await this.minioClient.bucketExists(bucketName)
        if(!isExists){
            await this.minioClient.makeBucket(bucketName, "us-east-1")
        }
        return bucketName
    }

    private isSupportedAssetType(assetType: AssetType) {
        return assetType === AssetType.Image || assetType === AssetType.TIFF || assetType === AssetType.PDF;
    }

    private async getUrl(fileName: string): Promise<string> {
        const bucketName = await this.getMinioBucket()
        const url = "http://"+ this.options["endPoint"] + ':'+ this.options['port'] + '/' + bucketName + '/' + fileName
        return url
    }

    private async getObjectContent(objectName: string): Promise<string>{
        return new Promise<string>(async(resolve, reject) =>{
            try {
                var objectData: string = ''
                const bucketName = await this.getMinioBucket()
                const dataStream = await this.minioClient.getObject(bucketName, objectName)
                dataStream.setEncoding('utf-8')
                //让文件流开始'流'动起来
                dataStream.resume();
                //监听读取的数据，data是文件的内容
                dataStream.on('data', data =>{
                    objectData += data
                })
                //监听状态
                dataStream.on('end', () => { 
                    console.log('文件读取结束')
                    resolve(objectData)
                })                
            } catch (error) {
                reject(error)               
            }
        })
    }

    private async getFileList(bucketName: string, objectPath: string): Promise<string[]> {
        return new Promise<string[]>((resolve, reject) => {
            try {
                var result: string[] = [];
                const stream = this.minioClient.listObjects(bucketName, '', true)
                stream.on('data', (data) => {
                    if (objectPath === '') {
                        result.push(data.name);
                    } else if (data.name.indexOf(objectPath) !== -1) {
                        result.push(data.name);
                    }
                })
                stream.on('end', ()=>{
                    console.log('读取结束')
                    resolve(result) 
                    // return result
                })            
            } catch (error) {
                reject(error)          
            }

        });
    }

    private minioStorageErrorHandler = (exception, ignoreNotFound?: boolean, ignoreForbidden?: boolean) => {
        const appError = this.toAppError(exception);
        throwUnhandledRejectionForEdge(appError, ignoreNotFound, ignoreForbidden);
        throw appError;
    }

    private toAppError(exception) {
        if (exception.statusCode === 404 || exception.code === "NoSuchKey") {
            return new AppError(
                ErrorCode.MinioBucketIONotFound,
                strings.errors.minioBucketIONotFound.message,
                strings.errors.minioBucketIONotFound.title);
        } else if (exception.statusCode === 403) {
            return new AppError(
                ErrorCode.BlobContainerIOForbidden,
                strings.errors.blobContainerIOForbidden.message,
                strings.errors.blobContainerIOForbidden.title);
        } else if (exception.code === "REQUEST_SEND_ERROR") {
            return new AppError(
                ErrorCode.RequestSendError,
                strings.errors.requestSendError.message,
                strings.errors.requestSendError.title,
            );
        }
        return exception;
    }
}