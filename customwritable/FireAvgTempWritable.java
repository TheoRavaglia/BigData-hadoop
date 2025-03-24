package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.checkerframework.checker.units.qual.Temperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


// vamos criar uma classe writable
// é serializavel indica que os dados possam ser persistiveis (armazenados) em disco
//ou transmitidos pela rede
//ou seja, conversão para binário(0s e 1s)

// por que é importante? Porque precisa estar bem escrita para sair de um map de um computador
// e ir para outro computador de no reduce

//JAVA Bean?
// java bean:
// 1.atributos privados ou protected
// 2. temos gets e sets para todos os atributos
// 3. construtor vazio (note que podemos ter outros construtores,
// mas precisamos sempre do vazio

// Já implementaram algo antes?

// IMPORTANTE: um writable precisa ser um bean


public class FireAvgTempWritable implements Writable{

    // nossa classe precisa ter o n de quantidade
    // nossa classe precisa ter a soma

    //atributos privados
    private int n;
    private float soma;

    //vamos criar o construtor vazio
    //botão direito e em generate-->constructor-->select none


    //criar o construtor vazio
    public FireAvgTempWritable() {
    }

    //botão direito e em generate-->constructor-->selecionar dois argumentos


    public FireAvgTempWritable(int n, float soma) {
        this.n = n;
        this.soma = soma;
    }

    // o que esta faltando: getter e setter
    //botão direito e em generate-->getter and setter

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public float getSoma() {
        return soma;
    }

    public void setSoma(float soma) {
        this.soma = soma;
    }


    // temos mais um detalhe, ajustar funções abaixo


    //na escrita
    //precisa fazer na mesma ordem, no caso n e depois soma

    // na mesma ordem, porque precisa ser igual nas duas funçoes
    //abaixo

    //IMPORTANTE: não troquem a ordem

    @Override
    public void write(DataOutput out) throws IOException {
        //escrever n e soma nesta ordem
        out.writeUTF(String.valueOf(n));
        out.writeUTF(String.valueOf(soma));
    }

    //le os campos, ou seja, lê os atributos
    @Override
    public void readFields(DataInput in) throws IOException {

        //seguindo a mesma ordem do write
        n = Integer.parseInt(in.readUTF());
        soma = Float.parseFloat(in.readUTF());
    }
}

//terminamos nosso primeiro writable...
