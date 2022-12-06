import core, data, strings, time, math, fun, locale, regex, bin, decimals from 'std';
import json from 'tweakstreet/json';

library d {
    in: {{ input_data }};
    config: {{ config_data }};
    f: {
        :parse_date  time.parser("uuuu-MM-dd")
    }; 
    rule_prefix: "(in, config, f) -> let { parse_date: f[:parse_date]; }";
    rule_input: 
~~~
{{ rule }}
~~~
;

}

library rules {
    rule: d.rule_prefix .. d.rule_input;
    eval:(string x) -> json.stringify(
            core.eval(rule) (d.in, d.config, d.f)
    );
}



