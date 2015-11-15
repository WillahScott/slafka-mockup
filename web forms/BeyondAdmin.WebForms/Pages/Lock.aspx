<%@ Page Title="" Language="C#" MasterPageFile="~/Empty.Master" AutoEventWireup="true" CodeBehind="Lock.aspx.cs" Inherits="BeyondAdmin.WebForms.Pages.Lock" %>
<asp:Content ID="Main" ContentPlaceHolderID="MainContent" runat="server">
    <div class="lock-container animated fadeInDown">
        <div class="lock-box text-align-center">
            <div class="lock-username">DIVYIA PHILLIPS</div>
            <img src="/assets/img/avatars/divyia.jpg" alt="divyia avatar" />
            <div class="lock-password">
                <form role="form" class="form-inline" action="/Default.aspx">
                    <div class="form-group">
                        <span class="input-icon icon-right">
                            <input class="form-control" placeholder="Password" type="password">
                            <i class="glyphicon glyphicon-log-in themeprimary"></i>
                        </span>
                    </div>
                </form>
            </div>

        </div>
        <div class="signinbox">
            <span>Sign in as a different user?</span>
            <a href="/Account/Login.aspx">Sign In Now</a>
        </div>
    </div>
</asp:Content>
<asp:Content ID="Scripts" ContentPlaceHolderID="PageScriptContent" runat="server">
</asp:Content>
