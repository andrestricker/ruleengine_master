import core , data, strings, time, math, fun, locale, regex, bin, decimals from 'std';
import json from 'tweakstreet/json';

library d {
    input_json: '{{input_data}}';
    config_json: '{{config_data}}';

    inputs: json.parse(input_json);
    
    configs: json.parse(config_json);
    
    rule: (config, input) -> {{rule}};

    get_matches: (input, configs, rule) ->   
        data.reduce(configs, {:input input, :match [], :non_match []}, (a, config) -> 
        if rule(config, input) then
            {...a, :match [...a[:match], config]}
        else
            {...a, :non_match [...a[:non_matches], config]}
        );

    has_matches: (test) -> data.size(test[:match]) > 0 ;

    perform: (inputs, configs, rule) -> 
        let {
            tests: data.map(inputs, (input) -> get_matches(input, configs, rule));
        }
        {
            :matches     data.mapcat(tests, (t) -> if has_matches(t) [{:input t[:input], :match t[:match]}] else "skip"),
            :non_matches data.mapcat(tests, (t) -> if !has_matches(t) [t[:input]] else "skip")
        };

    rule_result: perform(inputs, configs, rule);
    
}

library rules {
    eval:(string x) -> json.stringify(
            d.rule_result 
    );
}



