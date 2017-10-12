package no.nav.opptjening.hoi.internal;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class NaisEndpoints {

    @RequestMapping(method = {RequestMethod.GET}, value="/isAlive")
    public String isAlive() {
        return "OK";
    }

    @RequestMapping(method = {RequestMethod.GET}, value="/isReady")
    public String isReady() {
        return "OK";
    }
}
