import org.springframework.core.io.ClassPathResource;

import com.inadco.hbl.compiler.Pig8CubeIncrementalCompilerBean;

public class Example1 {

    public static void main(String[] args) throws Exception {

        Pig8CubeIncrementalCompilerBean compiler =
            new Pig8CubeIncrementalCompilerBean(new ClassPathResource("example1.yaml"), new ClassPathResource(
                "example1-preambula.pig"),5);
        
        System.out.println(compiler.preparePigSource("$output"));

    }

}
