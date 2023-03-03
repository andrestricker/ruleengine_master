import core, data, strings, time, math, fun, locale, regex, bin, decimals from 'std';
import json from 'tweakstreet/json';

library d {
    input_json: '{{input_data}}';
    config_json: '{{config_data}}';

    input: json.parse(input_json);
    
    config: json.parse(config_json);
    
    is_config_match: (input, config) -> {{rule}} ;
    
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



