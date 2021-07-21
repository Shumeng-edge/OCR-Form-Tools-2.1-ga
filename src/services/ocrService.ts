// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

import Guard from "../common/guard";
import { IProject } from "../models/applicationState";
import { IStorageProvider, StorageProviderFactory } from "../providers/storage/storageProviderFactory";
import { constants } from "../common/constants";
import ServiceHelper from "./serviceHelper";
import { strings } from "../common/strings";
import { getAPIVersion } from "../common/utils";

export enum OcrStatus {
    loadingFromAzureBlob = "loadingFromAzureBlob",
    runningOCR = "runningOCR",
    done = "done",
    failed = "failed",
}

/**
 * @name - OCR Service
 * @description - Functions for dealing with OCR
 */
export class OCRService {
    private storageProviderInstance: IStorageProvider;

    constructor(private project: IProject) {
        Guard.null(project);
    }

    /**
     * get recognized text from OCR service
     * @param filePath - filepath sent to OCR
     * @param fileName - name of OCR file
     */
    public async getRecognizedText(
        filePath: string,
        fileName: string,
        mimeType: string,
        onStatusChanged?: (ocrStatus: OcrStatus) => void,
        rewrite?: boolean
    ): Promise<any> {
        Guard.empty(filePath);
        Guard.empty(this.project.apiUriBase);

        const notifyStatusChanged = (ocrStatus: OcrStatus) => onStatusChanged && onStatusChanged(ocrStatus);
        const ocrFileName = decodeURIComponent(`${fileName}${constants.ocrFileExtension}`);

        let ocrJson;
        try {
            notifyStatusChanged(OcrStatus.loadingFromAzureBlob);
            ocrJson = await this.readOcrFile(ocrFileName);
            if (!this.isValidOcrFormat(ocrJson) || rewrite) {
                ocrJson = await this.fetchOcrUriResult(filePath, fileName, ocrFileName, mimeType);
            }
        } catch (e) {
            notifyStatusChanged(OcrStatus.runningOCR);
            ocrJson = await this.fetchOcrUriResult(filePath, fileName, ocrFileName, mimeType);
        } finally {
            if (ocrJson) {
                notifyStatusChanged(OcrStatus.done);
            }
            else {
                notifyStatusChanged(OcrStatus.failed);
            }
        }
        return ocrJson;
    }

    /**
     * Get Storage Provider from project's target connection
     */
    protected get storageProvider(): IStorageProvider {
        if (!this.storageProviderInstance) {
            this.storageProviderInstance = StorageProviderFactory.create(
                this.project.sourceConnection.providerType,
                this.project.sourceConnection.providerOptions,
            );
        }

        return this.storageProviderInstance;
    }

    private readOcrFile = async (ocrFileName: string) => {
        const json = await this.storageProvider.readText(ocrFileName, true);
        if (json !== null) {
            return new Promise((resolve, reject) => {
                resolve(JSON.parse(json));
            });
        }
    }

    private fetchOcrUriResult = async (filePath: string, fileName: string, ocrFileName: string, mimeType: string) => {
        try {
            let body;
            let headers;
            let apiUrl;
            if (filePath.startsWith("file:")) {
                const bodyAndType = await Promise.all(
                    [
                        this.storageProvider.readBinary(decodeURI(fileName)),
                        this.storageProvider.getFileType(decodeURI(fileName))
                    ]
                );
                body = bodyAndType[0];
                headers = { "Content-Type": mimeType, "cache-control": "no-cache" };
            } else {
                body = { 
                    url: filePath,
                    apikey: 'LVvw38lAt4BTBAK1801PjcCOM2BlfsWpbEJKu11D',
                    images: await this.getBase64Image(filePath),
                    usertoken: '31df12f75be6d190ec06ba676b0ac393', 
                };
                // console.log(body)
                headers = { "Content-Type": "application/json;charset=UTF-8" };
            }
            const apiVersion = getAPIVersion(this.project?.apiVersion);
            if (this.project.apiUriBase.indexOf('azure')!==-1){
                apiUrl = this.project.apiUriBase + `/formrecognizer/${apiVersion}/layout/analyze`;
            } else {
                apiUrl = this.project.apiUriBase
            }
            const response = await ServiceHelper.postWithAutoRetry(
                apiUrl,
                body,
                { headers },
                this.project.apiKey as string,
            );

            if (response.headers["operation-location"]){
                const operationLocation = response.headers["operation-location"];
                return this.poll(
                    () => ServiceHelper.getWithAutoRetry(operationLocation, { headers }, this.project.apiKey as string),
                    120000,
                    1500).then(async (data) => {
                        // console.log(data)
                        await this.save(ocrFileName, data);
                        return data;
                    });
            }else {
                return this.getResponseData(response, 120000, 2000).then(async (data) => {
                    let lines: string[] = [];
                    let result;
                    for(const item of data.results){
                        result = {
                            boundingBox: (item.text_region.join(',')).split(',').map(Number),
                            text: item.text,
                            confidence: item.confidence
                        }
                        lines.push(result)
                    }
                    const dataConversion = {
                        code: data.code,
                        msg: data.msg,
                        analyzeResult: {
                            version: apiVersion,
                            readResults: [{
                                page: 1,
                                angle: 0,
                                width: data.width,
                                height: data.height,
                                unit: "pixel",
                                lines: lines
                            }],
                            pageResults: [
                                {
                                    page: 1,
                                    tables: []
                                }
                            ]
                        },
                    }

                    // console.log('dataConversion', dataConversion)
                    await this.save(ocrFileName, dataConversion);
                    return dataConversion;
                })
            }
        } catch (error) {
            if (error?.toJSON()?.message === "Network Error" || error.response.status === 400) {
                throw new Error(strings.errors.getOcrError.message);
            } else {
                throw new Error(error);
            }
        }
    }

    /**
     * Save OCR
     * @param metadata - Metadata for asset
     */
    private async save(fileName: string, ocrJson: any): Promise<any> {
        Guard.empty(fileName);
        Guard.null(ocrJson);

        await this.storageProvider.writeText(fileName, JSON.stringify(ocrJson, null, 4));
        return ocrJson;
    }

    /**
     * Poll function to repeatly check if request succeeded
     * @param func - function that will be called repeatly
     * @param timeout - timeout
     * @param interval - interval
     */
    private poll = (func, timeout, interval): Promise<any> => {
        const endTime = Number(new Date()) + (timeout || 10000);
        interval = interval || 100;

        const checkSucceeded = (resolve, reject) => {
            const ajax = func();
            ajax.then((response) => {
                if (response.data.status.toLowerCase() === constants.statusCodeSucceeded) {
                    resolve(response.data);
                } else if (Number(new Date()) < endTime) {
                    // If the request isn't succeeded and the timeout hasn't elapsed, go again
                    setTimeout(checkSucceeded, interval, resolve, reject);
                } else {
                    // Didn't succeeded after too much time, reject
                    reject(new Error("Timed out for getting Layout results"));
                }
            });
        };

        return new Promise(checkSucceeded);
    }

    private getResponseData = (response, timeout, interval): Promise<any> =>{
        const endTime = Number(new Date()) + (timeout || 10000);
        interval = interval || 100;
        const checkSucceeded =((resolve, reject)=>{
            if (response.data.msg.toLowerCase() === "success") {
                resolve(response.data);
            } else if (Number(new Date()) < endTime) {
                // If the request isn't succeeded and the timeout hasn't elapsed, go again
                setTimeout(checkSucceeded, interval, resolve, reject);
            } else {
                // Didn't succeeded after too much time, reject
                reject(new Error("Timed out for getting Layout results"));
            }
        });
        return new Promise(checkSucceeded)
    }

    private async getBase64Image(imgUrl: string): Promise<string[]>{
        var img = new Image();
        return new Promise<string[]> ((resolve, reject)=>{
            img.src = imgUrl;
            img.onload = (()=>{
                const canvas = document.createElement("canvas");
                canvas.width = img.width;
                canvas.height = img.height;
                const ctx = canvas.getContext("2d");
                ctx.drawImage(img, 0, 0);
                const dataURL = canvas.toDataURL('image/png');
                const result: string[] = [];
                result.push(dataURL.replace(/^data:image\/(png|jpg);base64,/, ""));
                // console.log(dataURL)
                resolve(result);
            })
            img.onerror = reject
            img.setAttribute('crossOrigin', 'anonymous')
        })
    }

    private isValidOcrFormat = (ocr): boolean => {
        return ocr && ocr.analyzeResult && ocr.analyzeResult.readResults;
    }
}
