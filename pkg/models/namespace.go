// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

type Namespace struct {
	Id        string `json:"id"`
	Password  string `json:"password"`
	KeyPrefix string `json:"key_prefix"`
}

func (n *Namespace) Encode() []byte {
	return jsonEncode(n)
}
