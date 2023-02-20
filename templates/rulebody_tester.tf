import core, data, strings, time, math, fun, locale, regex, bin, decimals from 'std';
import json from 'tweakstreet/json';

library d {
    input_json: '[
  {
    "input": "a"
  },
  {
    "input": "b"
  },
  {
    "input": "c"
  },
  {
    "input": "d"
  },
  {
    "input": "e"
  }
]';
    config_json: '[
  {
    "config": "config_a",
    "value": "a"
  },
  {
    "config": "config_b",
    "value": "b"
  },
  {
    "config": "config_bb",
    "value": "bb"
  },
  {
    "config": "config_c",
    "value": "c"
  }
]';
    input: json.parse(input_json);
    
    config: json.parse(config_json);
    
    is_config_match: (input, config) -> input[:input] == config[:value] || strings.starts_with?(config[:value], input[:input]);
    
    get_configs: (x) -> data.filter(config, (c) -> is_config_match(x, c));
    
    rule_result: 
        data.mapcat(input, (x) -> 
            let {
                cs: get_configs(x);
            }
            if (cs == []) then nil
            [
                {
                    :input x,
                    :result cs
                }
            ]
        );
}

library rules {
    eval:(string x) -> json.stringify(
            d.rule_result 
    );
}



