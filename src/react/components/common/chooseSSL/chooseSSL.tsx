// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

import React, { SyntheticEvent } from "react";

/**
 * Security Token Picker Properties
 * @member id - The id to bind to the input element
 * @member value - The value to bind to the input element
 * @member onChange - The event handler to call when the input value changes
 */


export interface IChooseSSLProps {
    id?: string;
    value: any;
    onChange: (value: string) => void;
}

/**
 * Security Token Picker
 * @description - Used to display a list of security tokens
 */
export class ChooseUseSSL extends React.Component<IChooseSSLProps> {
    constructor(props) {
        super(props);

        this.state = { value: '' };

        this.onChange = this.onChange.bind(this);
    }

    public render() {
        return (
            <div>
                <label style = {{marginRight: "15px"}}><input type='radio' value = "false" checked={this.props.value === 'false'} onChange={this.onChange} /> http </label>
                <label><input type='radio' value = "true" checked={this.props.value === 'true'} onChange={this.onChange}  /> https </label>
            </div>
        );
    }

    private onChange(e: SyntheticEvent) {
        const inputElement = e.target as HTMLSelectElement;
        this.props.onChange(inputElement.value ? inputElement.value : undefined);
    }
}
