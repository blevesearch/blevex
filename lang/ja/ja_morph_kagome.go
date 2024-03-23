//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package ja

import (
	"fmt"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/ikawaha/kagome-dict/dict"

	"github.com/ikawaha/kagome-dict/ipa"
	"github.com/ikawaha/kagome/v2/tokenizer"
)

const TokenizerName = "kagome"

type KagomeMorphTokenizer struct {
	tok *tokenizer.Tokenizer
}

var defaultSystemDict = ipa.DictShrink()

func NewKagomeMorphTokenizer() (*KagomeMorphTokenizer, error) {
	t, err := tokenizer.New(defaultSystemDict, tokenizer.OmitBosEos())
	if err != nil {
		return nil, fmt.Errorf("failed to create kagome tokenizer: %v", err)
	}
	return &KagomeMorphTokenizer{
		tok: t,
	}, nil
}

func NewKagomeMorphTokenizerWithUserDic(user *dict.UserDict) (*KagomeMorphTokenizer, error) {
	t, err := tokenizer.New(defaultSystemDict, tokenizer.UserDict(user), tokenizer.OmitBosEos())
	if err != nil {
		return nil, fmt.Errorf("failed to create kagome tokenizer: %v", err)
	}
	return &KagomeMorphTokenizer{
		tok: t,
	}, nil
}

func (t *KagomeMorphTokenizer) Tokenize(input []byte) analysis.TokenStream {
	if len(input) < 1 {
		return analysis.TokenStream{}
	}
	morphs := t.tok.Analyze(string(input), tokenizer.Search)
	ret := make(analysis.TokenStream, len(morphs))
	for i, m := range morphs {
		ret[i] = &analysis.Token{
			Term:     []byte(m.Surface),
			Position: i,
			Start:    m.Position,
			End:      m.Position + len(m.Surface),
			Type:     analysis.Ideographic,
		}
	}
	return ret
}

func KagomeMorphTokenizerConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.Tokenizer, error) {
	return NewKagomeMorphTokenizer()
}

func init() {
	registry.RegisterTokenizer(TokenizerName, KagomeMorphTokenizerConstructor)
}
