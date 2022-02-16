package ecommerce.alura.main;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
        emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try {

            //sem ligar para seguranca referente ao amount
            var email = req.getParameter("email");
            var orderId = UUID.randomUUID().toString();
            var amount = new BigDecimal(req.getParameter("amount"));
            var order = new Order(orderId, amount, email);
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);


            var emailCode = new Email("Pedido em processamento!", "Obrigado pelo pedido! Ja estamos a processa-lo");
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);

            System.out.println("Novo pedido enviado com sucesso! ");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Novo pedido enviado! ");
        } catch (ExecutionException e)
        {
            throw new ServletException(e);
        } catch (InterruptedException e)
        {
            throw new ServletException(e);
        }
    }
}
